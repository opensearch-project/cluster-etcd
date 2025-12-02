#!/bin/bash
set -e

echo "=== Starting Cluster Controller ==="

# 1. Set runtime defaults based on your requirements
# Use PORT env var, default to 8080 (from your launch script)
HTTP_PORT=${PORT:-8080}
# Use ETCD_ENDPOINT env var, default to host.docker.internal
DEFAULT_ETCD_ENDPOINT="http://host.docker.internal:2379"
ETCD_ENDPOINT_TO_USE=${ETCD_ENDPOINT:-${DEFAULT_ETCD_ENDPOINT}}

# 2. Port configuration from your launch_cluster_controller.sh
# HOSTNAME is automatically set inside the container
NODE_NAME=${NODE_NAME:-${HOSTNAME}}
CLUSTER_NAME=${CLUSTER_NAME:-"default-docker-cluster"}

# Set log/cache dir paths (must match Dockerfile)
#export LOG_DIR="/var/log/cluster-controller"
#export CACHE_DIR="/tmp/cluster-controller-cache"
export CONFIG_DIR="/app/conf"

# 3. Configure JVM Options
# We combine options from your script with the new runtime overrides
export JAVA_OPTS="-XX:+UseG1GC \
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -Dserver.port=$HTTP_PORT \
  -DNODE_NAME=$NODE_NAME \
  -DCLUSTER_NAME=$CLUSTER_NAME \
  -Detcd.endpoint=$ETCD_ENDPOINT_TO_USE \
  -Dmanagement.endpoints.web.exposure.include=health,info \
  -Dmanagement.endpoint.health.show-details=always \
  -Dmanagement.endpoints.web.base-path=/ \
  -Dmanagement.server.port=$HTTP_PORT"
  # Add any other -D properties here

echo "--- Configuration ---"
echo "HTTP Port:       $HTTP_PORT"
echo "Node Name:       $NODE_NAME"
echo "Cluster Name:    $CLUSTER_NAME"
echo "Etcd Endpoint:   $ETCD_ENDPOINT_TO_USE"
if [ -n "$CONFIG_DIR" ]; then
    echo "Config Dir:      $CONFIG_DIR (using external application.yml)"
fi
echo "---------------------"
echo "JAVA_OPTS: $JAVA_OPTS"
echo "---------------------"

# 4. Execute the application

# Check for a debug flag. If set to "true", sleep indefinitely.
if [ "$DEBUG_KEEP_ALIVE" = "true" ]; then
  echo "--- DEBUG_KEEP_ALIVE is true ---"
  echo "Container will sleep indefinitely."
  echo "To debug, run: docker exec -it <container_id> /bin/bash"
  echo "Once inside, you can manually run:"
  echo "java $JAVA_OPTS -jar /app/app.jar"
  echo "----------------------------------"
  exec sleep infinity
else
  if [ -n "$CONFIG_DIR" ]; then
      # ... ensure absolute path ...
      export CONTROLLER_CONFIG_FILE="${CONFIG_DIR}/application.yml" # Sets the env var for Java
      echo "External Config File Env Var: CONTROLLER_CONFIG_FILE=$CONTROLLER_CONFIG_FILE"
  else
      unset CONTROLLER_CONFIG_FILE
      echo "External Config File Env Var: Not Set (using classpath)"
  fi

  exec java $JAVA_OPTS -jar "/app/app.jar" # Java reads CONTROLLER_CONFIG_FILE
fi