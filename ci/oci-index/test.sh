#!/busybox/sh
# Exercises oci-index.sh against crane's in-memory registry. Runs in the plugin's own
# base image (crane/debug), locally or as a CI step:
#   docker run --rm -v "$PWD:/src" -w /src --entrypoint /busybox/sh <crane/debug image> ci/oci-index/test.sh
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
REG=localhost:5000
fails=0

crane registry serve --address "$REG" >/tmp/registry.log 2>&1 &
i=0
until crane catalog "$REG" >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -lt 50 ] || { echo "registry did not start"; cat /tmp/registry.log; exit 1; }
  sleep 0.1
done

echo hello > /tmp/payload
tar -C /tmp -cf /tmp/layer.tar payload
# single_arch REF OS/ARCH: pushes a one-layer image whose config declares that platform.
single_arch() {
  crane append --oci-empty-base -f /tmp/layer.tar -t "$1" >/dev/null 2>&1
  crane mutate "$1" --set-platform "$2" >/dev/null 2>&1
}
single_arch "$REG/src:amd64" linux/amd64
single_arch "$REG/src:arm64" linux/arm64
single_arch "$REG/src:also-amd64" linux/amd64
crane pull --format oci "$REG/src:amd64" /tmp/layout-amd64
crane pull --format oci "$REG/src:arm64" /tmp/layout-arm64

# plugin SOURCES PLATFORMS TARGET TAGS: runs the plugin as Woodpecker would, with settings
# as PLUGIN_* variables; returns its exit status.
plugin() {
  PLUGIN_SOURCES=$1 PLUGIN_PLATFORMS=$2 PLUGIN_TARGET=$3 PLUGIN_TAGS=$4 \
    /busybox/sh "$HERE/oci-index.sh" >/tmp/plugin.out 2>&1
}
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; sed 's/^/    /' /tmp/plugin.out; fails=$((fails + 1)); }
exists() { crane digest "$1" >/dev/null 2>&1; }
says() { grep -q "$1" /tmp/plugin.out; }
same() { [ "$(crane digest "$1")" = "$(crane digest "$2")" ]; }
# child IMAGE OS/ARCH: digest the index resolves for that platform (fails if it has none).
child() { crane digest --platform "$2" "$1" 2>/dev/null; }
# is_index_of_src IMAGE: the index resolves amd64 and arm64 to exactly the src images.
is_index_of_src() {
  [ "$(child "$1" linux/amd64)" = "$(crane digest "$REG/src:amd64")" ] \
    && [ "$(child "$1" linux/arm64)" = "$(crane digest "$REG/src:arm64")" ]
}
# rejects NAME REPO PATTERN SOURCES PLATFORMS: the run must fail, name PATTERN as the
# reason, and leave REPO:latest unwritten.
rejects() {
  if plugin "$4" "$5" "$REG/$2:abc" latest; then bad "$1: accepted"
  elif exists "$REG/$2:latest"; then bad "$1: moved an extra tag anyway"
  elif ! says "$3"; then bad "$1: failed for another reason"
  else ok "$1"; fi
}

if plugin "$REG/src:amd64,$REG/src:arm64" linux/amd64,linux/arm64 "$REG/app:abc" latest,stable \
  && is_index_of_src "$REG/app:abc" && same "$REG/app:latest" "$REG/app:abc" && same "$REG/app:stable" "$REG/app:abc"; then
  ok "two single-arch images become one index resolving each platform to its source, under every tag"
else
  bad "the index does not resolve each platform to its source, or an extra tag misses the index"
fi

if plugin "/tmp/layout-amd64=$REG/lay:abc-amd64,/tmp/layout-arm64=$REG/lay:abc-arm64" linux/amd64,linux/arm64 "$REG/lay:abc" latest \
  && is_index_of_src "$REG/lay:abc" && same "$REG/lay:abc-amd64" "$REG/src:amd64" && same "$REG/lay:latest" "$REG/lay:abc"; then
  ok "LAYOUT=REF sources are pushed to REF, then indexed"
else
  bad "layout sources were not pushed and indexed"
fi

if plugin "$REG/src:arm64,$REG/src:amd64" linux/amd64,linux/arm64 "$REG/order:abc" latest && is_index_of_src "$REG/order:abc"; then
  ok "source order does not have to follow platform order"
else
  bad "a reordered source list was rejected or mis-indexed"
fi

rejects "an index missing an expected platform fails" wrongarch "do not match the expected" \
  "$REG/src:amd64,$REG/src:also-amd64" linux/amd64,linux/arm64
rejects "an index carrying an extra platform entry fails" extra "do not match the expected" \
  "$REG/src:amd64,$REG/src:arm64,$REG/src:also-amd64" linux/amd64,linux/arm64
rejects "a missing source fails" nosrc "cannot resolve source: $REG/src:missing" \
  "$REG/src:amd64,$REG/src:missing" linux/amd64,linux/arm64
if exists "$REG/nosrc:abc"; then bad "a missing source still wrote the index"; fi
rejects "a missing layout fails" nolayout "could not push layout /tmp/nope" \
  "/tmp/nope=$REG/nolayout:abc-amd64,$REG/src:arm64" linux/amd64,linux/arm64
rejects "sources with no entries fail" nosources "sources is required" "," linux/amd64,linux/arm64
rejects "platforms with no entries fail" noplatforms "platforms is required" "$REG/src:amd64" ","

if plugin "$REG/src:amd64,$REG/src:arm64" linux/amd64,linux/arm64 "" latest; then
  bad "an empty target was accepted"
elif ! says "target is required"; then
  bad "the missing target was not the reported failure"
else
  ok "a missing target fails"
fi

# A crane on PATH that refuses to run if the password reaches its argv, which any
# process on the host can read from /proc.
mkdir -p /tmp/shim
cat > /tmp/shim/crane <<EOF
#!/busybox/sh
case " \$* " in *s3cret-token*) echo "password on argv: \$*" >&2; exit 97 ;; esac
exec $(command -v crane) "\$@"
EOF
chmod +x /tmp/shim/crane
if PATH=/tmp/shim:$PATH PLUGIN_REGISTRY=$REG PLUGIN_USERNAME=ci PLUGIN_PASSWORD=s3cret-token \
  plugin "$REG/src:amd64,$REG/src:arm64" linux/amd64,linux/arm64 "$REG/authed:abc" ""; then
  stored=$(echo "$REG" | crane auth get)
  if says "s3cret-token"; then bad "the password appeared in the plugin output"
  elif [ "$stored" != '{"Username":"ci","Secret":"s3cret-token"}' ]; then bad "login stored [$stored], not the configured credential"
  else ok "logs in with the configured credential, never printing it or passing it on argv"; fi
else
  bad "the login path exited non-zero"
fi

if PLUGIN_PASSWORD=s3cret-token plugin "$REG/src:amd64,$REG/src:arm64" linux/amd64,linux/arm64 "$REG/noreg:abc" ""; then
  bad "a password without registry and username was accepted"
elif ! says "password needs registry and username"; then
  bad "the incomplete login settings were not the reported failure"
else
  ok "a password without registry and username fails"
fi

[ "$fails" -eq 0 ] || { echo "$fails test(s) failed"; exit 1; }
echo "all oci-index tests passed"
