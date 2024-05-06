#!/usr/bin/env bash

set -euo pipefail
[[ ${DEBUG-} != true ]] || set -x

: "${HBOX_HOME:="$(dirname -- "$0")"/../../hbox-1.7.0-SNAPSHOT}"
: "${HBOX_CONF_DIR:="$(dirname -- "$0")"/../conf.hpc-yarn}"
export HBOX_CONF_DIR

submit_opts=(--app-name "[HBOX][test] yarn submit")
submit_opts+=(--app-type  "TENSORNET")
submit_opts+=(--worker-num 2)
submit_opts+=(--worker-cores 2)
submit_opts+=(--worker-memory 8G)

if [[ ${USE_LOCAL_MPI-} == true ]]; then
  submit_opts+=(--conf hbox.use.cached.mpi.package=false)
fi

# command run on containers
submit_opts+=(--hbox-cmd)
submit_opts+=("/bin/sh -xc hostname;pwd;whoami;{ls,-l,.,tmp};{ps,f};cat<launch_container.sh;{which,hadoop};env")

exec "$HBOX_HOME"/bin/hbox-submit "${submit_opts[@]}"
