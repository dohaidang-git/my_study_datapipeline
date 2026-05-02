FROM apache/hive:4.0.1

ARG POSTGRES_JDBC_VERSION=42.7.5

ADD --chmod=0644 https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

USER root

RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hadoop/share/hadoop/common/lib/ \
 && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hadoop/share/hadoop/common/lib/ \
 && cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/ \
 && cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/ \
 && chown hive:hive /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.6.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

USER hive
