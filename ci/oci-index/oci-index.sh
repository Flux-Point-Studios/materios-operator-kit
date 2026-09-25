#!/busybox/sh
# Woodpecker plugin: publishes images from credential-less builds, verifies what it
# published carries exactly the expected platforms, then applies the extra tags.
# Settings:
#   sources    one entry per platform: an image ref, or LAYOUT=REF to first push the
#              OCI image layout at LAYOUT to REF. Building credential-less into a
#              layout keeps registry credentials away from the Dockerfile's RUN steps.
#   platforms  os/arch list the result must carry, no more and no fewer. One platform
#              publishes its single source at target as a plain image, the same kind
#              of manifest a single-arch build always pushed; several publish an index.
#   target     ref the result is pushed to
#   tags       extra tags moved to target only after it verifies (optional)
#   registry, username, password   login, when password is set (optional)
#
# With a password, the plugin runs only on a push or manual pipeline of the default
# branch, logs in only to OCI_INDEX_REGISTRY and writes only under OCI_INDEX_PREFIX.
# Those come from the environment, not from settings: Woodpecker refuses a secret to
# any step that sets its own environment, so no pipeline can widen them.
set -euf

die() { echo "oci-index: $*" >&2; exit 1; }
list() { echo "$1" | tr ',' '\n' | sed '/^$/d'; }
words() { sort | tr '\n' ' '; }

SOURCES=$(list "${PLUGIN_SOURCES:-}")
PLATFORMS=$(list "${PLUGIN_PLATFORMS:-}")
TARGET=${PLUGIN_TARGET:-}
[ -n "$SOURCES" ] || die "sources is required"
[ -n "$PLATFORMS" ] || die "platforms is required"
[ -n "$TARGET" ] || die "target is required"

if [ -n "${PLUGIN_PASSWORD:-}" ]; then
  [ -n "${PLUGIN_REGISTRY:-}" ] && [ -n "${PLUGIN_USERNAME:-}" ] || die "password needs registry and username"
  default=${CI_REPO_DEFAULT_BRANCH:-}
  case "${CI_PIPELINE_EVENT:-}:${CI_COMMIT_BRANCH:-}" in
    push:"$default" | manual:"$default") [ -n "$default" ] ;;
    *) false ;;
  esac || die "the token is only used on a push or manual pipeline of the default branch"
  registry=${OCI_INDEX_REGISTRY:-ghcr.io}
  prefix=${OCI_INDEX_PREFIX:-ghcr.io/flux-point-studios/}
  [ "$PLUGIN_REGISTRY" = "$registry" ] || die "the token may only be sent to $registry"
  for ref in $TARGET $(echo "$SOURCES" | sed 's/^[^=]*=//'); do
    case "$ref" in "$prefix"*) ;; *) die "$ref is outside $prefix" ;; esac
  done
  printf '%s' "$PLUGIN_PASSWORD" | crane auth login "$registry" -u "$PLUGIN_USERNAME" --password-stdin >/dev/null
fi

set --
for src in $SOURCES; do
  case "$src" in
    *=*)
      layout=${src%%=*}; src=${src#*=}
      crane push "$layout" "$src" >/dev/null || die "could not push layout $layout to $src"
      ;;
  esac
  crane digest "$src" >/dev/null || die "cannot resolve source: $src"
  set -- "$@" -m "$src"
done

if [ "$(echo "$PLATFORMS" | wc -l)" -eq 1 ]; then
  [ "$#" -eq 2 ] || die "one platform takes exactly one source"
  src=$2
  [ "$src" = "$TARGET" ] || crane copy "$src" "$TARGET" >/dev/null
  manifest=$(crane manifest "$TARGET") || die "cannot read the manifest of $TARGET"
  # crane config resolves an index to its linux/amd64 child, so without this an index
  # carrying any amd64 entry would pass an amd64 check.
  echo "$manifest" | jq -e '.config' >/dev/null || die "$src is an index, not a single image"
  got=$(crane config "$TARGET" | jq -r '.os + "/" + .architecture' | words)
else
  crane index append "$@" -t "$TARGET"
  manifest=$(crane manifest "$TARGET") || die "cannot read the manifest of $TARGET"
  # crane writes each entry's platform from the source image's config, so this checks
  # what was actually built, not what the sources were named.
  got=$(echo "$manifest" | jq -r '.manifests[].platform | .os + "/" + .architecture' | words)
fi
want=$(echo "$PLATFORMS" | words)
[ "$got" = "$want" ] || die "platforms [${got% }] do not match the expected [${want% }]; extra tags left unchanged"

for tag in $(list "${PLUGIN_TAGS:-}"); do
  crane tag "$TARGET" "$tag"
done
echo "oci-index: $TARGET -> $(crane digest "$TARGET") [${got% }]${PLUGIN_TAGS:+, tagged $PLUGIN_TAGS}"
