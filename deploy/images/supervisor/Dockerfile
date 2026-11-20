# The supervisor: its runtime classpath and the etcd backend, staged by `./gradlew -p deploy stage`.
FROM eclipse-temurin:21-jre

RUN groupadd --system --gid 10001 supervisor \
    && useradd --system --uid 10001 --gid 10001 --no-create-home supervisor

COPY lib/ /opt/supervisor/lib/

USER 10001
WORKDIR /tmp
# Every option can also be given as SUPERVISOR_<OPTION>, which is how the Kubernetes manifests set them.
# It is read-only unless told --write (SUPERVISOR_WRITE=true).
ENTRYPOINT ["java", "-cp", "/opt/supervisor/lib/*", "org.opensearch.supervisor.app.Main"]
