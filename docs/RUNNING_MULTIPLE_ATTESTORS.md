# Running Multiple Attestors on One Host

If you operate multiple Cardano SPO pools (or just want redundancy), you can
run more than one Materios attestor on the same machine. Each attestor runs
as a completely independent process with its own:

- Install directory (and mnemonic file)
- Docker Compose project
- Cert-daemon container
- Health-endpoint port
- On-chain SS58 identity

The key is the `--install-dir` flag on `install.sh`, which is available as
of this commit. Before that flag existed, running the installer twice would
overwrite the first install — and along with it, the mnemonic file and any
accumulated cert-daemon state. If that happened to you, see
"Recovering from an overwritten install" below.

## Quick recipe

```bash
# First attestor — uses the default directory
bash install.sh --mode attestor --label first-attestor

# Second attestor — picks a different directory AND a different label
bash install.sh \
  --mode attestor \
  --label second-attestor \
  --install-dir ~/materios-attestor-2

# Third, if you really need one
bash install.sh \
  --mode attestor \
  --label third-attestor \
  --install-dir ~/materios-attestor-3
```

Each `--install-dir` must:
- Be distinct from every other install on the same host
- Point at an empty directory (or not exist yet — the installer will `mkdir -p` it)
- Be reachable on disk from the user running Docker

Each `--label` must be distinct too, because the label is what shows up in the
Materios explorer committee list. Running two attestors with the same label
makes it impossible to tell them apart, and the heartbeat store on the gateway
will see them stomping on each other.

The health endpoint runs on port 8080 inside each cert-daemon container and is
published to `127.0.0.1:8080` on the host by default. If you want to inspect
each attestor's health endpoint directly, you'll need to change the host-side
port mapping — edit the `ports:` block in each install's `docker-compose.yml`
and set e.g. `"127.0.0.1:8081:8080"` for the second attestor. This is a
manual step; the installer does not pick a free port automatically.

## How it works under the hood

`install.sh` derives a Docker Compose project name from the basename of the
install directory (lowercased, with non-alphanumeric characters replaced by
dashes). That project name is written into the generated `docker-compose.yml`
as a top-level `name:` field and exported as `COMPOSE_PROJECT_NAME` in the
shell. Docker Compose then uses that project name as the prefix for all
container and volume names.

For example, given:

| Install dir                     | Project name            | Container names                                          | Volume names                     |
|---------------------------------|-------------------------|----------------------------------------------------------|----------------------------------|
| `~/materios-attestor`           | `materios-attestor`     | `materios-attestor-cert-daemon-1`                        | `materios-attestor_cert-daemon-data` |
| `~/materios-attestor-2`         | `materios-attestor-2`   | `materios-attestor-2-cert-daemon-1`                      | `materios-attestor-2_cert-daemon-data` |

Because the prefixes differ, Docker happily runs both side by side. Without
the per-project prefix, the second install would either fail to start (name
collision on `cert-daemon-1`) or overwrite the first's containers.

## Managing multiple installs

Because `docker compose` commands are CWD-aware, you can manage each install
by `cd`-ing into its directory:

```bash
# Attestor 1 logs
cd ~/materios-attestor && docker compose logs -f cert-daemon

# Attestor 2 logs
cd ~/materios-attestor-2 && docker compose logs -f cert-daemon

# Restart attestor 2 only
cd ~/materios-attestor-2 && docker compose restart
```

Or use the explicit `-p` flag from anywhere on the host:

```bash
docker compose -p materios-attestor-2 logs -f cert-daemon
docker compose -p materios-attestor-2 restart
```

## Updating a specific install

`update.sh` also accepts `--install-dir`:

```bash
curl -sSL https://raw.githubusercontent.com/Flux-Point-Studios/materios-operator-kit/main/update.sh \
  | bash -s -- --install-dir ~/materios-attestor-2
```

If you run `update.sh` without `--install-dir`, it will pick the first install
it finds at `~/materios-operator` or `~/materios-attestor` (the default
paths), which is almost certainly NOT what you want when multiple installs
exist. Always pass `--install-dir` for non-default installs.

## Re-running install.sh against an existing install

If you re-run `install.sh --install-dir <existing-dir>`, the installer will:

1. Detect the existing `docker-compose.yml` in that directory.
2. Warn you and prompt for confirmation (interactive sessions only). If you
   say no, it aborts with no changes.
3. Stop the running containers for that project.
4. Wipe the cert-daemon data volume (blobs + certs + daemon-state.json). This
   is intentional — it prevents heartbeats from reporting stale chain state
   after a chain reset. Blobs and certs re-fetch automatically.
5. Re-use the mnemonic and api-key files from the existing install so the
   operator's on-chain identity is preserved.

If you were trying to create a *new* independent attestor and ran with the
same `--install-dir` by accident, answer "no" at the prompt and try again
with a different directory.

## Edge cases and gotchas

- **Labels**: each install needs its own `--label`. If two attestors share a
  label, the explorer committee view can't tell them apart and the gateway's
  per-label rate limits will be shared across both.
- **Health port**: the default `127.0.0.1:8080:8080` mapping will collide.
  Edit the second install's `docker-compose.yml` and pick e.g. 8081 for the
  host side before `docker compose up -d`. The container side stays at 8080.
- **Faucet drip**: each attestor is a separate SS58 address, so each triggers
  its own faucet drip. If the faucet is rate-limited, the second install's
  drip may be rejected — the daemon retries on startup.
- **Legacy cleanup**: `install.sh` stops containers from prior installs in
  other directories by default, to recover operators who switched between
  validator and attestor modes. That cleanup is **skipped** when you pass
  `--install-dir` explicitly, precisely so the first install's containers
  are not touched when you install the second.
- **macOS `realpath`**: BSD `realpath` on macOS doesn't support
  `--canonicalize-missing`, so the installer falls back to a `python3`
  one-liner (`os.path.abspath(os.path.expanduser(...))`). You need Python 3
  on macOS if you're passing `--install-dir`.

## Recovering from an overwritten install

This flag exists because one operator lost their first attestor's cert
progress when they ran the installer a second time from the same HOME. If
that just happened to you:

1. The mnemonic was overwritten too, so the first attestor's SS58 identity
   is effectively lost unless you had backed up the mnemonic file beforehand.
   Check `~/materios-attestor/.secret-mnemonic.bak` (the installer does not
   create backups, but you may have).
2. Your accumulated cert-daemon state (`daemon-state.json`) is gone. This is
   not catastrophic — the daemon re-joins the committee from whatever block
   is current, and certification resumes. You do lose the historical state
   showing continuous uptime from the old install.
3. Going forward, always pass `--install-dir` when you want a second
   attestor, and back up `.secret-mnemonic` immediately after each install.

## Manual smoke-test procedure

The unit-test in `tests/test_install_dir.sh` verifies the flag-parsing and
project-name derivation logic without pulling images or hitting the network.
For a full end-to-end check with real Docker containers, run the following
manually on a disposable host (this WILL pull real images, register with the
faucet, and start daemons):

```bash
# Clean slate
rm -rf ~/materios-attestor ~/materios-attestor-2

# Install the first attestor
bash install.sh --mode attestor --label smoke-test-1
# When complete, note the SS58 address printed in the summary.

# Install the second attestor
bash install.sh --mode attestor --label smoke-test-2 \
  --install-dir ~/materios-attestor-2

# Verify both are running side-by-side
docker ps --format '{{.Names}}' | grep cert-daemon
# Expected output:
#   materios-attestor-cert-daemon-1
#   materios-attestor-2-cert-daemon-1

# Verify distinct mnemonics
diff ~/materios-attestor/.secret-mnemonic ~/materios-attestor-2/.secret-mnemonic \
  && echo "FAIL: mnemonics are identical" \
  || echo "OK: mnemonics differ"

# Verify distinct volumes
docker volume ls --format '{{.Name}}' | grep cert-daemon-data
# Expected:
#   materios-attestor_cert-daemon-data
#   materios-attestor-2_cert-daemon-data

# Tear down
cd ~/materios-attestor && docker compose down -v
cd ~/materios-attestor-2 && docker compose down -v
rm -rf ~/materios-attestor ~/materios-attestor-2
```
