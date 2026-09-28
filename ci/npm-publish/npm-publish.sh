# Woodpecker plugin: publishes npm tarballs that an earlier step packed without the token.
# Settings:
#   dir    absolute path of the directory holding the .tgz files to publish
#   token  npm token
#
# The token is used only on the first run of a push pipeline of the default branch, only for
# packages under SCOPES, and only against REGISTRY, with no package script run. A manual run
# and a restart (CI_PIPELINE_PARENT other than 0) carry variables chosen by whoever starts
# them, which reach this step's environment, so both are refused before any program starts.
# This script runs in a statically linked busybox that LD_PRELOAD cannot load code into, and
# starts every other program with an environment it sets itself.
set -eu
# The default field separators, whatever the environment set: printf is a builtin.
IFS=$(printf ' \t\nx')
IFS=${IFS%x}
BB=/static/busybox
REGISTRY=https://registry.npmjs.org/
SCOPES="@orynq @fluxpointstudios"

die() { echo "npm-publish: $*" >&2; exit 1; }

default=${CI_REPO_DEFAULT_BRANCH:-}
case "${CI_PIPELINE_EVENT:-}:${CI_COMMIT_BRANCH:-}" in
  push:"$default") [ -n "$default" ] ;;
  *) false ;;
esac || die "the token is only used on a push pipeline of the default branch"
[ "${CI_PIPELINE_PARENT:-}" = 0 ] || die "the token is not used on a restarted pipeline"
[ -n "${PLUGIN_TOKEN:-}" ] || die "token is required"
DIR=${PLUGIN_DIR:-}
case "$DIR" in
  *[[:space:]]*) die "dir must not contain whitespace" ;;
  /*) ;;
  *) die "dir must be an absolute path" ;;
esac
[ -d "$DIR" ] || die "$DIR is not a directory"

run() { "$BB" env -i PATH=/usr/local/bin:/usr/bin:/bin HOME="$work" "$@"; }

work=$("$BB" mktemp -d)
trap '"$BB" rm -rf "$work"' EXIT
cd "$work"
(umask 077 && printf '//registry.npmjs.org/:_authToken=%s\n' "$PLUGIN_TOKEN" >"$work/npmrc")

# Every tarball is checked before any is published.
checked=
for tgz in "$DIR"/*.tgz; do
  [ -e "$tgz" ] || continue
  [ -f "$tgz" ] && [ ! -L "$tgz" ] || die "$tgz is not a regular file"
  # One word per field: a value holding whitespace yields extra words and is refused.
  fields=$("$BB" tar -xzOf "$tgz" package/package.json | run node -e '
    let s = "";
    process.stdin.on("data", (d) => (s += d)).on("end", () => {
      const p = JSON.parse(s);
      const r = (p.publishConfig || {}).registry;
      console.log([p.name, p.version, r === undefined ? "-" : r].join(" "));
    });') || die "$tgz does not hold a readable package/package.json"
  set -f
  set -- $fields
  set +f
  [ "$#" = 3 ] || die "$tgz: name, version and publishConfig.registry must not contain whitespace"
  name=$1 version=$2 registry=$3
  scope=${name%%/*} base=${name#*/}
  case " $SCOPES " in *" $scope "*) ;; *) die "$tgz: $name is outside $SCOPES" ;; esac
  case "$base" in "" | *[!a-z0-9._-]*) die "$tgz: $name is not a package name" ;; esac
  # A prerelease would need a dist-tag other than latest; none is published from here.
  case "$version" in [0-9]*.[0-9]*.[0-9]*) ;; *) die "$tgz: $version is not a release version" ;; esac
  case "$version" in *[!0-9.]*) die "$tgz: $version is not a release version" ;; esac
  case "$registry" in - | "${REGISTRY%/}" | "$REGISTRY") ;; *) die "$tgz: publishConfig.registry is $registry" ;; esac
  checked="$checked$tgz $name@$version
"
done
[ -n "$checked" ] || { echo "npm-publish: nothing to publish in $DIR"; exit 0; }

echo "$checked" | while read -r tgz package; do
  [ -n "$tgz" ] || continue
  # Packing ran any package scripts already; publishing a tarball runs none, and
  # --ignore-scripts keeps it that way.
  run npm publish "$tgz" --ignore-scripts --access public --registry "$REGISTRY" --userconfig "$work/npmrc" </dev/null \
    || die "npm did not publish $package"
  echo "npm-publish: published $package"
done
