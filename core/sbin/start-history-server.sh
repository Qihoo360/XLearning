#!/usr/bin/env bash
set -e

export HBOX_HOME="$(cd "`dirname "$0"`"/..; pwd)"

. "$HBOX_HOME"/conf/hbox-env.sh

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

HBOX_LIB_DIR=$HBOX_HOME/lib

num_jars="$(ls -1 "$HBOX_LIB_DIR" | grep "^hbox.*hadoop.*\.jar$" | wc -l)"
if [ "$num_jars" -eq "0" ]; then
  echo "Failed to find Hbox jar in $HBOX_LIB_DIR." 1>&2
  exit 1
fi
if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple Hbox jars in $HBOX_LIB_DIR:" 1>&2
  echo "$HBOX_LIB_DIR" 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

LAUNCH_CLASSPATH=$HBOX_CLASSPATH

nohup "$RUNNER" -cp "$LAUNCH_CLASSPATH" "net.qihoo.hbox.jobhistory.JobHistoryServer" "$@"  2>&1 &
