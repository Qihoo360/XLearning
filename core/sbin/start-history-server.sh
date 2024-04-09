#!/usr/bin/env bash

set -euo pipefail
[[ ${DEBUG-} != true ]] || set -x

HBOX_HOME="$(cd -- "$(dirname -- "$0")"/.. && pwd)"

# shellcheck source-path=SCRIPTDIR/..
. "$HBOX_HOME/conf/hbox-env.sh"

# Find the java binary
if [[ ${JAVA_HOME-} ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
  RUNNER="${JAVA_HOME}/bin/java"
elif hash java >/dev/null; then
  RUNNER=java
else
  echo "[ERROR] JAVA_HOME is not set" >&2
  exit 1
fi

HBOX_LIB_DIR="$HBOX_HOME/lib"

num_jars="$(find "$HBOX_LIB_DIR" -maxdepth 1 -name "hbox*hadoop*.jar" | wc -l)"
if (( num_jars == 0 )); then
  echo "[ERROR] Failed to find Hbox jar in $HBOX_LIB_DIR." >&2
  exit 1
elif (( num_jars > 1 )); then
  echo "[ERROR] Found multiple Hbox jars in $HBOX_LIB_DIR:" >&2
  find "$HBOX_LIB_DIR" -maxdepth 1 -name "hbox*hadoop*.jar" >&2
  echo "Please remove all but one jar." >&2
  exit 1
fi

# include HS jar
nohup "$RUNNER" -cp "$HBOX_CLASSPATH" net.qihoo.hbox.jobhistory.JobHistoryServer "$@" 2>&1 &
