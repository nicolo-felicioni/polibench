FROM mysql

MAINTAINER me

ENV MYSQL_ROOT_PASSWORD="password"

COPY initdb.sql /docker-entrypoint-initdb.d/

EXPOSE 3306