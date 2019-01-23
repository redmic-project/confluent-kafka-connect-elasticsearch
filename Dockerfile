FROM registry.gitlab.com/redmic-project/docker/kafka-connector-builder:latest AS builder

FROM maven:3.5.2-jdk-8

COPY --from=builder /root/.m2 /root/.m2

RUN mkdir -p /build

COPY pom.xml /build
COPY src /build/src

WORKDIR /build

RUN mvn package
