#!/bin/bash
set -e

trap 'docker rm -f pulsar-gen-native-config &> /dev/null || echo' EXIT

SHELL_DIR=$1
PULSAR_CONTAINER_IMAGE=${2:-apachepulsar/pulsar:2.10.0}

if [[ -z "$SHELL_DIR" ]]; then
  echo "Usage: generate-native-image-config.sh SHELL_DIR"
  exit 1
fi

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

NATIVE_CONFIG_PATH=$THIS_DIR/META-INF/native-image
PULSAR_EXTRA_OPTS="-agentlib:native-image-agent=config-merge-dir=${NATIVE_CONFIG_PATH}"

docker run --rm -d --name pulsar-gen-native-config -p 6650:6650 -p 8080:8080 $PULSAR_CONTAINER_IMAGE bin/pulsar standalone -nss -nfw
until curl -f -s http://localhost:8080/metrics/ > /dev/null; do
  sleep 3
done

echo """
  config list
  admin topics create mytopic
  client produce -m msg -n 10 mytopic
""" | PULSAR_EXTRA_OPTS=$PULSAR_EXTRA_OPTS $SHELL_DIR/bin/pulsar-shell - --fail-on-error

echo "GraalVM native image configurations have been updated in the directory $NATIVE_CONFIG_PATH"