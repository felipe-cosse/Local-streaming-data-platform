<?php

declare(strict_types=1);

function envOrDefault(string $name, string $default): string
{
    $value = getenv($name);
    return $value === false || $value === '' ? $default : $value;
}

function uuidV4(): string
{
    $data = random_bytes(16);
    $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
    $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}

function utcNow(): string
{
    return (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d\TH:i:s.v\Z');
}

$bootstrapServers = envOrDefault('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092');
$topicName = envOrDefault('KAFKA_TOPIC', 'app.php.events.v1');
$producerName = envOrDefault('PRODUCER_NAME', 'php-generator');
$rate = max((float) envOrDefault('EVENTS_PER_SECOND', '2'), 0.1);
mt_srand((int) envOrDefault('RANDOM_SEED', '42') + 1);

$configuration = new RdKafka\Conf();
$configuration->set('bootstrap.servers', $bootstrapServers);
$configuration->set('client.id', $producerName);
$configuration->set('enable.idempotence', 'true');
$configuration->set('acks', 'all');
$configuration->set('compression.type', 'zstd');
$configuration->setErrorCb(
    static function (RdKafka\Producer $producer, int $errorCode, string $reason): void {
        fwrite(STDERR, sprintf("Kafka error %d: %s\n", $errorCode, $reason));
    }
);

$producer = new RdKafka\Producer($configuration);
$topic = $producer->newTopic($topicName);
$running = true;

if (function_exists('pcntl_async_signals')) {
    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, static function () use (&$running): void { $running = false; });
    pcntl_signal(SIGINT, static function () use (&$running): void { $running = false; });
}

$types = ['user.activity', 'order.checkout', 'system.log', 'application.metric'];
$actions = ['login', 'search', 'view', 'logout'];
$levels = ['INFO', 'WARN', 'ERROR'];
$intervalMicroseconds = (int) (1_000_000 / $rate);
fwrite(STDOUT, sprintf("Producing %.2f events/s to %s\n", $rate, $topicName));

while ($running) {
    $started = hrtime(true);
    $eventType = $types[array_rand($types)];
    $partitionKey = (string) mt_rand(1, 100);
    $timestamp = utcNow();

    $payload = match ($eventType) {
        'user.activity' => [
            'user_id' => $partitionKey,
            'action' => $actions[array_rand($actions)],
            'ip' => sprintf('203.0.113.%d', mt_rand(1, 254)),
        ],
        'order.checkout' => [
            'customer_id' => $partitionKey,
            'amount' => number_format(mt_rand(500, 50000) / 100, 2, '.', ''),
            'currency' => 'USD',
        ],
        'system.log' => [
            'level' => $levels[array_rand($levels)],
            'component' => ['api', 'worker', 'billing'][mt_rand(0, 2)],
            'message' => 'Generated PHP application log event',
        ],
        default => [
            'metric' => ['latency_ms', 'queue_depth', 'request_count'][mt_rand(0, 2)],
            'value' => number_format(mt_rand(1000, 1000000) / 1000, 3, '.', ''),
            'unit' => 'count',
        ],
    };

    $event = [
        'event_id' => uuidV4(),
        'event_type' => $eventType,
        'event_version' => 1,
        'producer' => $producerName,
        'occurred_at' => $timestamp,
        'ingested_at' => $timestamp,
        'correlation_id' => uuidV4(),
        'partition_key' => $partitionKey,
        'payload' => $payload,
    ];

    $topic->produce(
        RD_KAFKA_PARTITION_UA,
        0,
        json_encode($event, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES),
        $partitionKey
    );
    $producer->poll(0);

    $elapsedMicroseconds = (int) ((hrtime(true) - $started) / 1000);
    if ($elapsedMicroseconds < $intervalMicroseconds) {
        usleep($intervalMicroseconds - $elapsedMicroseconds);
    }
}

for ($attempt = 0; $attempt < 5; $attempt++) {
    if ($producer->flush(2000) === RD_KAFKA_RESP_ERR_NO_ERROR) {
        break;
    }
}
