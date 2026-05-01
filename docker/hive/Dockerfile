FROM apache/hive:4.0.1

ARG POSTGRES_JDBC_VERSION=42.7.5

ADD --chmod=0644 https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar
