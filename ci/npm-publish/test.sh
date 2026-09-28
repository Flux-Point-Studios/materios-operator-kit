#!/bin/sh
# Exercises npm-publish.sh in the plugin's base image. It replaces npm with a recorder that
# hands each call on to the real npm as an offline dry run, and the plugin's static busybox with one
# that logs every program the plugin starts, so it runs only in a throwaway container:
#   docker run --rm -v "$PWD:/src" -w /src <node:24-alpine image> sh ci/npm-publish/test.sh
set -eu
[ -f /.dockerenv ] || { echo "npm-publish test: run it in a throwaway container"; exit 1; }

HERE=$(cd "$(dirname "$0")" && pwd)
TOKEN=npm_Tok3nThatMustStayInTheUserconfig
DIR=/tmp/pack
fails=0

mv /usr/local/bin/npm /usr/local/bin/npm-real
cat >/usr/local/bin/npm <<'EOF'
#!/bin/sh
{
  echo "cwd $(pwd)"
  echo "args $*"
  env | sort | sed 's/^/env /'
  prev=
  for a in "$@"; do [ "$prev" = --userconfig ] && sed 's/^/rc /' "$a"; prev=$a; done
} >>/tmp/npm.log
[ ! -e /tmp/npm-fails ] || exit 1
exec /usr/local/bin/npm-real "$@" --dry-run --offline
EOF
chmod +x /usr/local/bin/npm
mkdir -p /static
cat >/static/busybox <<'EOF'
#!/bin/sh
echo "$*" >>/tmp/programs.log
exec /bin/busybox "$@"
EOF
chmod +x /static/busybox

# pkg FILE JSON: a tarball in $DIR holding JSON as package/package.json.
pkg() {
  rm -rf /tmp/pkg && mkdir -p /tmp/pkg/package
  printf '%s\n' "$2" >/tmp/pkg/package/package.json
  tar -C /tmp/pkg -czf "$DIR/$1" package
}
fresh() {
  rm -rf "$DIR" /tmp/npm.log /tmp/programs.log /tmp/npm-fails /tmp/ran-* && mkdir -p "$DIR"
  : >/tmp/npm.log
  : >/tmp/programs.log
}
# plugin [VAR=VALUE ...]: runs the plugin as Woodpecker runs it on a first push of main,
# with the given variables changed; returns its exit status.
plugin() {
  env CI_PIPELINE_EVENT=push CI_COMMIT_BRANCH=main CI_REPO_DEFAULT_BRANCH=main CI_PIPELINE_PARENT=0 \
    PLUGIN_DIR="$DIR" PLUGIN_TOKEN="$TOKEN" "$@" sh "$HERE/npm-publish.sh" >/tmp/plugin.out 2>&1
}
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; sed 's/^/    /' /tmp/plugin.out; fails=$((fails + 1)); }
says() { grep -q -- "$1" /tmp/plugin.out; }
calls() { grep -c '^args ' /tmp/npm.log || true; }
# refuses NAME PATTERN [VAR=VALUE ...]: the run must fail naming PATTERN, without calling npm.
refuses() {
  name=$1 pattern=$2
  shift 2
  : >/tmp/npm.log
  : >/tmp/programs.log
  if plugin "$@"; then bad "$name: accepted"
  elif [ "$(calls)" != 0 ]; then bad "$name: npm was called"
  elif ! says "$pattern"; then bad "$name: failed for another reason"
  else ok "$name"; fi
}
# before_any_program NAME PATTERN [VAR=VALUE ...]: refused, and no program was started.
before_any_program() {
  refuses "$@"
  if [ -s /tmp/programs.log ]; then bad "$1: started $(head -1 /tmp/programs.log)"; fi
}

A='{"name":"@orynq/observe","version":"0.1.3","publishConfig":{"access":"public","registry":"https://registry.npmjs.org"}}'
B='{"name":"@fluxpointstudios/orynq-sdk-core","version":"0.2.0"}'

fresh
pkg a.tgz "$A"
pkg b.tgz "$B"
if plugin CANARY=1 NODE_OPTIONS=--require=/nonexistent LD_PRELOAD=/nonexistent.so \
  && [ "$(calls)" = 2 ] && says "published @orynq/observe@0.1.3" && says "published @fluxpointstudios/orynq-sdk-core@0.2.0"; then
  ok "each tarball is published once"
else
  bad "the tarballs were not each published once"
fi
rc=$(grep '^rc ' /tmp/npm.log | sort -u)
workdir=$(sed -n 's/^cwd //p' /tmp/npm.log | sort -u)
[ "$rc" = "rc //registry.npmjs.org/:_authToken=$TOKEN" ] && ok "the token is bound to registry.npmjs.org and nothing else is configured" \
  || bad "the userconfig held: $rc"
[ "$(grep '^args ' /tmp/npm.log | sort)" = "$(printf 'args publish %s --ignore-scripts --access public --registry https://registry.npmjs.org/ --userconfig %s/npmrc\n' "$DIR/a.tgz" "$workdir" "$DIR/b.tgz" "$workdir" | sort)" ] \
  && ok "npm publishes each tarball without scripts, public, to registry.npmjs.org" || bad "npm was called otherwise: $(grep '^args ' /tmp/npm.log)"
[ "$(grep '^env ' /tmp/npm.log | cut -d= -f1 | sort -u | tr '\n' ' ')" = "env HOME env PATH env PWD env SHLVL " ] \
  && ok "npm runs with only the environment the plugin sets" || bad "npm saw: $(grep '^env ' /tmp/npm.log | cut -d= -f1 | sort -u | tr '\n' ' ')"
[ ! -e "$workdir" ] && ok "the userconfig is removed" || bad "$workdir is left behind"
[ "$(echo "$workdir" | wc -l)" = 1 ] && [ -n "$workdir" ] && [ "${workdir#"$DIR"}" = "$workdir" ] \
  && ok "npm runs outside the packed directory" || bad "npm ran in $workdir"
if grep -q "$TOKEN" /tmp/plugin.out; then bad "the token reached the output"; else ok "the token stays out of the output"; fi

fresh
if plugin && [ "$(calls)" = 0 ] && says "nothing to publish"; then ok "an empty directory publishes nothing"
else bad "an empty directory was not a clean no-op"; fi

fresh
pkg a.tgz "$A"
before_any_program "a manual pipeline is refused" "push pipeline of the default branch" CI_PIPELINE_EVENT=manual
before_any_program "a pull request is refused" "push pipeline of the default branch" CI_PIPELINE_EVENT=pull_request
before_any_program "a push to another branch is refused" "push pipeline of the default branch" CI_COMMIT_BRANCH=feature
before_any_program "an unknown default branch is refused" "push pipeline of the default branch" CI_COMMIT_BRANCH= CI_REPO_DEFAULT_BRANCH=
before_any_program "a restarted pipeline is refused" "restarted pipeline" CI_PIPELINE_PARENT=7
before_any_program "a pipeline with no parent value is refused" "restarted pipeline" CI_PIPELINE_PARENT=
before_any_program "a missing token is refused" "token is required" PLUGIN_TOKEN=
before_any_program "a relative dir is refused" "absolute path" PLUGIN_DIR=pack
before_any_program "a dir holding whitespace is refused" "whitespace" "PLUGIN_DIR=$DIR /etc"
before_any_program "a missing dir is refused" "not a directory" PLUGIN_DIR=/tmp/absent

# rejected NAME PATTERN JSON: a tarball holding JSON is refused, and npm is not called.
rejected() {
  fresh
  pkg x.tgz "$3"
  refuses "$1" "$2"
}
rejected "a package outside the scopes is refused" "outside" '{"name":"@evil/observe","version":"1.0.0"}'
rejected "an unscoped package is refused" "outside" '{"name":"observe","version":"1.0.0"}'
rejected "a scope that only starts like an allowed one is refused" "outside" '{"name":"@orynqx/observe","version":"1.0.0"}'
rejected "a name with a path in it is refused" "not a package name" '{"name":"@orynq/../observe","version":"1.0.0"}'
rejected "a version that is not one is refused" "not a release version" '{"name":"@orynq/observe","version":"latest"}'
rejected "a version with a shell character is refused" "not a release version" '{"name":"@orynq/observe","version":"1.0.0;x"}'
rejected "a prerelease is refused" "not a release version" '{"name":"@orynq/observe","version":"1.0.0-rc.1"}'
rejected "a field holding whitespace is refused" "whitespace" '{"name":"@orynq/observe","version":"1.0.0 2.0.0"}'
rejected "a field holding a newline is refused" "whitespace" '{"name":"@orynq/observe\nx","version":"1.0.0"}'
rejected "another publishConfig.registry is refused" "publishConfig.registry" \
  '{"name":"@orynq/observe","version":"1.0.0","publishConfig":{"registry":"https://evil.example/"}}'
rejected "a tarball without a package.json is refused" "readable package" 'not json'

fresh
pkg a.tgz "$A"
ln -s "$DIR/a.tgz" "$DIR/link.tgz"
refuses "a symlinked tarball is refused" "not a regular file"

fresh
pkg a.tgz "$A"
pkg b.tgz "$B"
touch /tmp/npm-fails
if plugin; then bad "an npm failure was not a failure"
elif [ "$(calls)" != 1 ]; then bad "the plugin went on after npm failed"
elif ! says "npm did not publish @orynq/observe@0.1.3"; then bad "the failure does not name the package"
else ok "an npm failure stops the run and names the package"; fi

fresh
pkg s.tgz '{"name":"@orynq/observe","version":"1.0.0","scripts":{"prepack":"touch /tmp/ran-prepack","prepublishOnly":"touch /tmp/ran-prepublishOnly","prepare":"touch /tmp/ran-prepare","publish":"touch /tmp/ran-publish","postpublish":"touch /tmp/ran-postpublish","postpack":"touch /tmp/ran-postpack"}}'
if plugin && [ "$(calls)" = 1 ] && ! ls /tmp/ran-* >/dev/null 2>&1; then ok "no script of the published package runs"
else bad "a package script ran: $(ls /tmp/ran-* 2>/dev/null | tr '\n' ' ')"; fi

[ "$fails" -eq 0 ] || { echo "$fails npm-publish test(s) failed"; exit 1; }
echo "all npm-publish tests passed"
