FROM apache/spark:3.5.3

USER root
WORKDIR /opt/crt/generator

COPY target/mock-generator-1.0-SNAPSHOT.jar /opt/crt/generator/app.jar
COPY config /opt/crt/generator/config

ENTRYPOINT ["/opt/java/openjdk/bin/java"]
CMD ["-jar", "/opt/crt/generator/app.jar"]
