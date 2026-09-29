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

# Named in full: mktemp would otherwise create it under the step's TMPDIR, and npm reads the
# .npmrc of the nearest directory above it that holds a package.json.
work=$("$BB" mktemp -d /tmp/npm-publish.XXXXXX)
trap '"$BB" rm -rf "$work"' EXIT
cd "$work"
(umask 077 && printf '//registry.npmjs.org/:_authToken=%s\n' "$PLUGIN_TOKEN" >"$work/npmrc")

# npm reads a tarball's manifest with pacote, from whichever top directory holds it, and applies
# every publishConfig key it knows as configuration: a scoped registry there wins over
# --registry, and proxy or TLS keys reroute the request. The manifest is read here the same way,
# and only access "public" and registry keys naming REGISTRY are admitted.
MANIFEST='
const pacote = require("/usr/local/lib/node_modules/npm/node_modules/pacote");
const [tgz, registry] = process.argv.slice(1);
const admitted = ([key, value]) =>
  key === "access"
    ? value === "public"
    : /(^|:)registry$/.test(key) && (value === registry || value === registry.slice(0, -1));
pacote
  .manifest(tgz, { fullMetadata: true, fullReadJson: true })
  .then((p) => {
    const refused = Object.entries(p.publishConfig ?? {}).filter((entry) => !admitted(entry));
    if (refused.length > 0) {
      const listed = refused.map(([key, value]) => `${JSON.stringify(key)}: ${JSON.stringify(value)}`);
      throw new Error(`publishConfig ${listed.join(", ")} is not admitted`);
    }
    console.log(p.name, p.version);
  })
  .catch((e) => {
    console.error(`npm-publish: ${e.message}`);
    process.exitCode = 1;
  });
'

# Every tarball is checked before any is published. Other steps share $DIR and can run beside
# this one, so each tarball is copied where only this container reaches, and the copy is what is
# checked and published.
checked=
n=0
for tgz in "$DIR"/*.tgz; do
  [ -e "$tgz" ] || continue
  n=$((n + 1))
  copy="$work/$n.tgz"
  [ -f "$tgz" ] && [ ! -L "$tgz" ] && "$BB" cp -P "$tgz" "$copy" && [ -f "$copy" ] && [ ! -L "$copy" ] \
    || die "$tgz is not a regular file"
  fields=$(run node -e "$MANIFEST" "$copy" "$REGISTRY") || die "$tgz is refused"
  # One word per field: a value holding whitespace yields extra words and is refused.
  set -f
  set -- $fields
  set +f
  [ "$#" = 2 ] || die "$tgz: name and version must not contain whitespace"
  name=$1 version=$2
  scope=${name%%/*} base=${name#*/}
  case " $SCOPES " in *" $scope "*) ;; *) die "$tgz: $name is outside $SCOPES" ;; esac
  case "$base" in "" | *[!a-z0-9._-]*) die "$tgz: $name is not a package name" ;; esac
  # pacote admits only a valid semver version. A prerelease would need a dist-tag other than
  # latest; none is published from here.
  case "$version" in *[!0-9.]*) die "$tgz: $version is not a release version" ;; esac
  checked="$checked$copy $name@$version
"
done
[ -n "$checked" ] || { echo "npm-publish: nothing to publish in $DIR"; exit 0; }

echo "$checked" | while read -r copy package; do
  [ -n "$copy" ] || continue
  # Packing ran any package scripts already; publishing a tarball runs none, and
  # --ignore-scripts keeps it that way.
  run npm publish "$copy" --ignore-scripts --access public --registry "$REGISTRY" --userconfig "$work/npmrc" </dev/null \
    || die "npm did not publish $package"
  echo "npm-publish: published $package"
done
