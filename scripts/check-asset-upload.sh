#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
module=wisepen-ai-asset-service/wisepen-ai-asset-biz
sh mvnw -q -pl "$module" -am test dependency:build-classpath -Dmdep.outputFile=target/check-classpath.txt
java -cp "$module/target/test-classes:$module/target/classes:$(cat "$module/target/check-classpath.txt")" \
  com.oriole.wisepen.ai.asset.service.impl.VersionServiceUploadCheck "$@"
