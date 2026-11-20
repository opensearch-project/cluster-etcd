# The control plane: its runtime classpath and the etcd backend, staged by `./gradlew -p deploy stage`.
FROM eclipse-temurin:21-jre

# Not 1000, which some base images already give to a user of their own.
RUN groupadd --system --gid 10001 controlplane \
    && useradd --system --uid 10001 --gid 10001 --no-create-home controlplane

COPY lib/ /opt/control-plane/lib/

USER 10001
WORKDIR /tmp
EXPOSE 9250
# A wildcard classpath rather than installDist's start script, whose CLASSPATH lists the jars it was built
# with and so would not see the backend. Heap and other JVM flags: JDK_JAVA_OPTIONS.
ENTRYPOINT ["java", "-cp", "/opt/control-plane/lib/*", "org.opensearch.controlplane.Main"]
CMD ["--help"]
