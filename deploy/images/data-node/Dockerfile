# A clusterless data node: an OpenSearch distribution built from the same checkout as the plugin, with
#   opensearch-data-plane  the plugin, and the etcd backend dropped into its directory
#   ingestion-fs           the file ingestion source
#   repository-s3          the segment store (MinIO in the test cluster)
#   ingestion-kafka        the Kafka ingestion source
# Staged by `./gradlew -p deploy stage`; see deploy/README.md.
#
# Two stages, and no BuildKit-only syntax (RUN --mount, COPY --chmod): a Docker without buildx falls back to the
# legacy builder, which rejects them. The first stage carries the 240 MB tarball and the plugin zips, so the
# image keeps only the installed tree.

FROM debian:bookworm-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates unzip \
    && rm -rf /var/lib/apt/lists/*

COPY opensearch.tar.gz /tmp/
RUN mkdir /usr/share/opensearch \
    && tar -xzf /tmp/opensearch.tar.gz -C /usr/share/opensearch --strip-components=1 \
    && rm /tmp/opensearch.tar.gz

WORKDIR /usr/share/opensearch
COPY plugins/ /tmp/plugins/
RUN for plugin in opensearch-data-plane ingestion-fs repository-s3 ingestion-kafka; do \
        bin/opensearch-plugin install --batch "file:///tmp/plugins/${plugin}.zip" || exit 1; \
    done

# Into the plugin's directory and not lib/: nothing in the plugin names a backend, ServiceLoader finds it on the
# plugin's classloader, and lib/ is where a duplicated library fails the node (kv-store/BACKENDS.md).
COPY backend/ plugins/opensearch-data-plane/
# The same rule the node applies at startup, applied here so that a backend that duplicates one of the
# node's classes fails the image build, not every pod.
COPY check-jar-hell.sh /tmp/
RUN bash /tmp/check-jar-hell.sh lib plugins/opensearch-data-plane

FROM debian:bookworm-slim

# curl for probing a node from inside its pod. No JDK: the distribution bundles one.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# 1000 is the uid OpenSearch's own image uses, so volumes prepared for that image fit this one.
RUN groupadd --gid 1000 opensearch \
    && useradd --uid 1000 --gid 1000 --home-dir /usr/share/opensearch --no-create-home opensearch

COPY --from=build --chown=1000:1000 /usr/share/opensearch /usr/share/opensearch
COPY --chown=1000:1000 opensearch.yml /usr/share/opensearch/config/opensearch.yml
# Run through bash, so the script's mode does not matter (Docker only copies it through unchanged).
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

USER 1000
WORKDIR /usr/share/opensearch
EXPOSE 9200 9300
VOLUME /usr/share/opensearch/data
ENTRYPOINT ["bash", "/usr/local/bin/entrypoint.sh"]
