FROM php:8.4.11-cli-alpine3.22

RUN apk add --no-cache librdkafka \
    && apk add --no-cache --virtual .build-deps autoconf g++ make librdkafka-dev \
    && pecl install rdkafka-6.0.5 \
    && docker-php-ext-enable rdkafka \
    && apk del .build-deps

WORKDIR /app
COPY services/php/producer.php /app/producer.php

USER 65532:65532
CMD ["php", "/app/producer.php"]
