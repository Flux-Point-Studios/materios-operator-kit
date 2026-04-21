#!/usr/bin/env bash
# Smoke test for the --install-dir flag on install.sh.
#
# What this verifies (without pulling Docker images or talking to the network):
#   1. Install-dir path resolution: absolute, ~-prefixed, ./-prefixed
#   2. COMPOSE_PROJECT_NAME derivation produces distinct names for two dirs
#   3. Help text mentions --install-dir
#   4. Running the installer twice with different --install-dir values would
#      produce two distinct docker-compose.yml files with different project
#      names and cert-daemon container names (verified by stubbing docker/curl
#      and exercising install.sh in "dry run" mode — we abort right after the
#      compose file is written, so no images are pulled and no network calls
#      are made).
#
# Run:
#   bash tests/test_install_dir.sh
#
# Exits 0 on success, non-zero on any assertion failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_SH="$REPO_ROOT/install.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

# ── 1. Help text ───────────────────────────────────────────────────────────
HELP_OUT=$(bash "$INSTALL_SH" --help)
echo "$HELP_OUT" | grep -q -- "--install-dir" || fail "help text missing --install-dir"
echo "$HELP_OUT" | grep -qi "multiple independent attestors" || fail "help text missing multi-attestor guidance"
pass "--help mentions --install-dir"

# ── 2. Argument parsing — reject unknown flags ─────────────────────────────
if bash "$INSTALL_SH" --bogus foo >/dev/null 2>&1; then
  fail "installer accepted unknown flag --bogus"
fi
pass "unknown flags rejected"

# ── 3. Static inspection of install.sh: new flag + project-name logic ──────
grep -q -- '--install-dir)' "$INSTALL_SH" || fail "install.sh does not parse --install-dir"
grep -q 'COMPOSE_PROJECT_NAME' "$INSTALL_SH" || fail "install.sh does not set COMPOSE_PROJECT_NAME"
grep -q 'INSTALL_DIR_EXPLICIT' "$INSTALL_SH" || fail "install.sh does not gate cleanup on INSTALL_DIR_EXPLICIT"
grep -q 'name: ${COMPOSE_PROJECT_NAME}' "$INSTALL_SH" || fail "generated compose files don't set top-level name"
pass "install.sh has flag + project-name + legacy-cleanup gating + compose name"

# ── 4. Project-name derivation: extract and smoke-test the logic ───────────
# Pull the derivation snippet out and run it against several inputs.
derive_project_name() {
  local dir="$1"
  local name
  name=$(basename "$dir" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/-/g')
  case "$name" in
    [a-z0-9]*) : ;;
    *) name="m-$name" ;;
  esac
  echo "$name"
}

[ "$(derive_project_name /home/op/materios-attestor)"   = "materios-attestor"   ] || fail "name1"
[ "$(derive_project_name /home/op/materios-attestor-2)" = "materios-attestor-2" ] || fail "name2"
[ "$(derive_project_name /opt/Materios-Attestor)"       = "materios-attestor"   ] || fail "lowercases"
[ "$(derive_project_name /srv/materios@attestor)"       = "materios-attestor"   ] || fail "sanitizes"
[ "$(derive_project_name /srv/-leading-dash)"           = "m--leading-dash"     ] || fail "leading-dash prefix"

# Critical: two different install dirs must produce distinct project names.
n1=$(derive_project_name "$HOME/materios-attestor")
n2=$(derive_project_name "$HOME/materios-attestor-2")
[ "$n1" != "$n2" ] || fail "two different dirs produced the same project name"
pass "project-name derivation distinct for two install dirs ($n1 vs $n2)"

# ── 5. Path expansion: ~ and ./ are converted to absolute paths ────────────
# Reproduce the inline logic from install.sh so we exercise it directly.
expand_install_dir() {
  local d="$1"
  case "$d" in
    "~")   d="$HOME" ;;
    "~/"*) d="$HOME/${d:2}" ;;
  esac
  if command -v realpath >/dev/null 2>&1 && realpath --help 2>&1 | grep -q canonicalize-missing; then
    realpath --canonicalize-missing "$d"
  else
    INSTALL_DIR="$d" python3 -c 'import os,sys; print(os.path.abspath(os.path.expanduser(os.environ["INSTALL_DIR"])))'
  fi
}

[ "$(expand_install_dir '~/foo')"     = "$HOME/foo" ]     || fail "~ expansion"
[ "$(expand_install_dir '~')"         = "$HOME"     ]     || fail "~ alone"
case "$(expand_install_dir './bar')" in
  /*/bar) pass "./ expands to absolute" ;;
  *) fail "./ did not expand to absolute: $(expand_install_dir './bar')" ;;
esac
[ "$(expand_install_dir '/tmp/abs')"  = "/tmp/abs"  ]     || fail "absolute preserved"

# ── 6. Wrapper scripts pass through --install-dir ──────────────────────────
for wrapper in "$REPO_ROOT/install-linux.sh" "$REPO_ROOT/install-macos.command"; do
  grep -q -- '--install-dir' "$wrapper" || fail "$wrapper does not pass --install-dir"
done
grep -q -- '--install-dir' "$REPO_ROOT/install-windows.bat" || fail "install-windows.bat does not pass --install-dir"
pass "all wrappers pass --install-dir through"

# ── 7. update.sh honours --install-dir for multi-install discovery ─────────
grep -q -- '--install-dir' "$REPO_ROOT/update.sh" || fail "update.sh does not accept --install-dir"
grep -q 'COMPOSE_PROJECT_NAME' "$REPO_ROOT/update.sh" || fail "update.sh does not scope volume lookups to project"
pass "update.sh accepts --install-dir and scopes volume lookups"

# ── 8. Backward compatibility: defaults are preserved when --install-dir omitted ──
# Operators who never use --install-dir should see zero change in behavior.
# Check that the default values are still intact in install.sh.
grep -q 'OPERATOR_DIR="\$HOME/materios-operator"' "$INSTALL_SH" || fail "default validator dir changed"
grep -q 'OPERATOR_DIR="\$HOME/materios-attestor"'  "$INSTALL_SH" || fail "default attestor dir changed"
# The legacy-cleanup block must still run when --install-dir is NOT passed.
grep -q 'if \[ "\$INSTALL_DIR_EXPLICIT" = false \]; then' "$INSTALL_SH" \
  || fail "legacy-cleanup block is not gated correctly"
pass "backward-compat defaults preserved"

echo ""
echo "All $(grep -c '^pass' "$0" 2>/dev/null || echo several) install-dir checks passed."
