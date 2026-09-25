#!/busybox/sh
# Extracts into DIR the jq the plugin image carries (from the image its Dockerfile
# copies it out of), for running oci-index.sh outside that image: its tests and the
# pull-request rehearsal.
set -eu
dir=${1:?usage: fetch-jq.sh DIR}
image=$(sed -n 's|^COPY --from=\([^ ]*\) /jq .*|\1|p' "$(dirname "$0")/Dockerfile")
[ -n "$image" ] || { echo "fetch-jq: the Dockerfile names no jq image" >&2; exit 1; }
mkdir -p "$dir"
crane export --platform linux/amd64 "$image" - | tar -x -C "$dir" jq
