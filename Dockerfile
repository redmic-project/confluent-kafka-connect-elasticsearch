FROM registry.gitlab.com/redmic-project/docker/kafka-connector-builder:dev-latest AS builder

FROM maven:3-jdk-8

COPY --from=builder /root/.m2 /root/.m2

RUN ls /root/.m2/repository

COPY pom.xml .
COPY src ./src

RUN mvn package
