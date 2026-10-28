#!/usr/bin/env bash
# 从空 Docker 环境恢复 CRT 基础运行面：中间件、Kafka topic、SeaweedFS lake bucket。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE="$ROOT/infra/docker-compose.yml"
AWS_SDK_JAR="$ROOT/infra/jars/aws-java-sdk-bundle-1.12.262.jar"

[[ -f "$ROOT/infra/seaweedfs/s3.json" ]] || {
  echo "missing infra/seaweedfs/s3.json; copy infra/seaweedfs/s3.example.json first" >&2
  exit 1
}
[[ -f "$AWS_SDK_JAR" ]] || {
  echo "missing $AWS_SDK_JAR; run Maven dependency:copy-dependencies first" >&2
  exit 1
}

docker compose -f "$COMPOSE" up -d kafka seaweedfs redis

for service in kafka redis; do
  ready=false
  for _ in $(seq 1 30); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "crt-${service}" 2>/dev/null || echo missing)"
    if [[ "$status" == healthy ]]; then
      ready=true
      break
    fi
    sleep 2
  done
  [[ "$ready" == true ]] || {
    echo "container crt-${service} did not become healthy" >&2
    exit 1
  }
done

for topic in content_events content_groundtruth; do
  docker exec crt-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

# SeaweedFS 新版基础镜像不自动创建业务桶；通过 AWS SDK 幂等创建。
jshell --class-path "$AWS_SDK_JAR" <<'EOF'
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.*;
var client = AmazonS3ClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("http://localhost:8333", "us-east-1")).withPathStyleAccessEnabled(true).withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("crtadmin", "crtadmin123"))).build();
if (!client.doesBucketExistV2("crt-lake")) client.createBucket("crt-lake");
System.out.println("BUCKET crt-lake ready");
/exit
EOF

docker compose -f "$COMPOSE" ps
