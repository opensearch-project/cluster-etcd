#-------------------------------------------------------------------
# STAGE 1: Build Stage
#
# This stage uses a full JDK and Maven image to:
# 1. Install Git
# 2. Clone the repo (using build-time ARGs)
# 3. Build the Spring Boot executable JAR
#-------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build

# Install Git, which is not in the base maven image
RUN apt-get update && apt-get install -y git vim

# Define build-time arguments with defaults
# These can be overridden during the build:
#   --build-arg REPO_URL="http://your-git-repo.com"
#   --build-arg GIT_HASH="your-commit-sha"
# TODO - update this default REPO_URL after moving the repo
ARG REPO_URL="https://github.com/TejasNaikk/cluster-controller.git"
ARG GIT_HASH="main"

WORKDIR /build

# Clone the specified repo and branch/hash
# --depth 1 for a shallow clone is faster
RUN git clone --depth 1 -b ${GIT_HASH} ${REPO_URL} .

# Build the application using Maven
# This will download dependencies and create the executable JAR
RUN mvn clean package spring-boot:repackage -DskipTests=true

#-------------------------------------------------------------------
# STAGE 2: Final Runtime Stage
#
# This stage uses a slim JRE image for a smaller, more secure
# final container.
#-------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# 1. Copy all app files first (while still root)
COPY --from=build /build/target/cluster-controller-*.jar /app/app.jar
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN mkdir -p /app/conf
COPY application.yml /app/conf/application.yml

# 2. Create user and directories (while still root)
RUN useradd -m -s /bin/bash appuser
RUN mkdir -p /var/log/cluster-controller /tmp/cluster-controller-cache

# 3. Set all permissions and ownership at once (while still root)
#    This is the key change: we chown AND chmod *before* switching user.
RUN chown -R appuser:appuser /app /var/log/cluster-controller /tmp/cluster-controller-cache \
    && chmod +x /app/docker-entrypoint.sh

# 4. NOW switch to the non-root user
USER appuser

# --- END FIX ---

# Expose the default port. This can be overridden at runtime.
EXPOSE 8080

# Run the entrypoint script when the container starts
ENTRYPOINT ["/app/docker-entrypoint.sh"]

