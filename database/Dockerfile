# Используем официальный PostgreSQL 13 версии образ как родительский образ
FROM postgres:13

# Установим переменные окружения PostgreSQL
ENV POSTGRES_DB database
ENV POSTGRES_USER username
ENV POSTGRES_PASSWORD secret
ENV POSTGRES_HOST db
ENV POSTGRES_PORT 5432

CMD ["postgres"]