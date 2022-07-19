FROM bitnami/cassandra:latest

COPY ./src/cassandra/cassandra_healthcheck.sh /home/

HEALTHCHECK CMD ["docker-healthcheck"]