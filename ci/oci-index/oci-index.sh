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
  printf '%s' "$PLUGIN_PASSWORD" | crane auth login "$PLUGIN_REGISTRY" -u "$PLUGIN_USERNAME" --password-stdin >/dev/null
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
  # crane reads an index's first matching child as its config, so an index here would
  # verify as whichever platform it happens to lead with.
  crane manifest "$TARGET" | jq -e '.config' >/dev/null || die "$src is an index, not a single image"
  got=$(crane config "$TARGET" | jq -r '.os + "/" + .architecture' | words)
else
  crane index append "$@" -t "$TARGET"
  # crane writes each entry's platform from the source image's config, so this checks
  # what was actually built, not what the sources were named.
  got=$(crane manifest "$TARGET" | jq -r '.manifests[].platform | .os + "/" + .architecture' | words)
fi
want=$(echo "$PLATFORMS" | words)
[ "$got" = "$want" ] || die "platforms [${got% }] do not match the expected [${want% }]; extra tags left unchanged"

for tag in $(list "${PLUGIN_TAGS:-}"); do
  crane tag "$TARGET" "$tag"
done
echo "oci-index: $TARGET -> $(crane digest "$TARGET") [${got% }]${PLUGIN_TAGS:+, tagged $PLUGIN_TAGS}"
