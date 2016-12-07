FROM openjdk:8-jre-alpine
MAINTAINER bikeemotion

ENV VERSION 1.0-SNAPSHOT

RUN mkdir -p /opt/gravedigger/boot/ && \
	apk add --update ca-certificates curl && \
    curl -L https://github.com/Bikeemotion/k8s-grave-digger/releases/download/v${VERSION}/gravedigger-bootstrapper-${VERSION}-jar-with-dependencies.jar -o /opt/gravedigger/boot/gravedigger-bootstrapper-${VERSION}-jar-with-dependencies.jar && \
    apk del curl

CMD ["java", "-jar", "/opt/gravedigger/boot/gravedigger-bootstrapper-1.0-SNAPSHOT-jar-with-dependencies.jar"]