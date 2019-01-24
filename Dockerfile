FROM registry.gitlab.com/redmic-project/docker/kafka-connector-builder:latest AS builder

FROM maven:3.5.2-jdk-8

LABEL maintainer="info@redmic.es"

COPY --from=builder /root/.m2 /root/.m2

RUN mkdir -p /build /jar

COPY pom.xml /build
COPY src /build/src

WORKDIR /build

ARG TARGET_PATH=target/redmic-kafka-connect-elasticsearch-5.0.1-package/share/java/kafka-connect-elasticsearch

RUN mvn package && \
	mv /build/${TARGET_PATH}/*.jar /jar && \
	rm /jar/guava-18.0.jar
