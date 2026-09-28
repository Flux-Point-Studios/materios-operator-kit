#!/usr/bin/env python3
"""Keep the GHCR publish token as repository-level Woodpecker secrets.

The token (secret name gchr_token) is held only by the repositories that publish through the
ci/oci-index plugin, and each copy admits only the plugin's pinned digests.

  plan  [--repo OWNER/NAME ...]
  apply --token-file PATH [--repo OWNER/NAME ...]
  pin   (--add IMAGE@sha256:DIGEST | --drop IMAGE@sha256:DIGEST)

apply first proves the token may push to the plugin's own package (it opens an upload session
there and cancels it). It then stores the token on exactly the named repositories (by default, the
ones already holding a copy) with the filter of the copy it replaces, checks what Woodpecker
stored, and only then deletes every global, organization and other repository copy. Run it with a
new token to rotate. pin changes the plugin digests every copy admits, the filter step of a plugin
upgrade.

Woodpecker lets any of its users with push on a repository edit that repository's secrets, where
only an admin may edit a global one. A repository copy's filter is therefore only as fixed as the
set of Woodpecker users: plan and apply name every user who is not an admin.

Environment: WOODPECKER_SERVER, WOODPECKER_TOKEN_FILE (a file holding an admin API token),
GITHUB_API_URL (default https://api.github.com), REGISTRY_URL (default https://ghcr.io). Each must use
https, or plain http to a loopback address. No redirect is followed.
"""
import argparse
import base64
import ipaddress
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from http.client import HTTPException

SECRET = "gchr_token"
PLUGIN_LOGIN = "realdecimalist"
PACKAGES_ORG = "Flux-Point-Studios"
PACKAGE_SCOPES = {"write:packages", "read:packages"}
REGISTRY = "ghcr.io"
PLUGIN_PACKAGE = "flux-point-studios/materios-operator-kit"
PINNED = re.compile(re.escape(f"{REGISTRY}/{PLUGIN_PACKAGE}") + r"@sha256:[0-9a-f]{64}")
CLASSIC_TOKEN = re.compile(r"ghp_[A-Za-z0-9]{36}")


class Refused(Exception):
    pass


class RefuseRedirects(urllib.request.HTTPRedirectHandler):
    """urllib copies every request header, Authorization included, onto the request a redirect makes."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise Refused(f"{req.get_method()} {req.full_url}: HTTP {code} redirect not followed")


OPENER = urllib.request.build_opener(RefuseRedirects)


def is_loopback(host):
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def http(method, url, auth, body=None):
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != "https" and not (parts.scheme == "http" and is_loopback(parts.hostname or "")):
        raise Refused(f"{method} {url}: credentials leave this machine only over https")
    request = urllib.request.Request(
        url, method=method, data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": auth, "Content-Type": "application/json", "User-Agent": "registry-token/1"})
    try:
        with OPENER.open(request, timeout=30) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()
    except OSError as e:
        raise Refused(f"{method} {url}: {getattr(e, 'reason', e)}") from None
    except (ValueError, HTTPException) as e:
        # Their messages can quote a header value, and the Authorization header holds a token.
        raise Refused(f"{method} {url}: malformed request or response ({type(e).__name__})") from None


def read_secret_file(path):
    try:
        with open(path) as f:
            value = f.read().strip()
    except OSError as e:
        raise Refused(f"cannot read {path}: {e.strerror}") from None
    if not value:
        raise Refused(f"{path}: token file is empty")
    if not re.fullmatch(r"[\x21-\x7e]+", value):
        raise Refused(f"{path}: the file must hold one token on one line")
    return value


class Woodpecker:
    def __init__(self, server, token_file):
        self._auth = "Bearer " + read_secret_file(token_file)
        self.api = server.rstrip("/") + "/api"

    def __repr__(self):
        return f"Woodpecker({self.api})"

    def call(self, method, path, body=None):
        status, _, raw = http(method, self.api + path, self._auth, body)
        if status >= 300:
            reply = raw.decode(errors="replace")
            if body and body.get("value"):
                reply = reply.replace(body["value"], "<token>")
            raise Refused(f"{method} {path}: HTTP {status} {reply[:200]}")
        return json.loads(raw) if raw.strip() else None

    def listing(self, path):
        items, page = [], 1
        while True:
            batch = self.call("GET", f"{path}?perPage=50&page={page}") or []
            items += batch
            if len(batch) < 50:
                return items
            page += 1

    def inventory(self):
        """All repositories, and every copy of the secret with the API path that addresses it."""
        repos = self.listing("/repos")
        # The organization listing leaves out users' organizations; only the user listing names those.
        orgs = sorted({r["org_id"] for r in repos} | {o["id"] for o in self.listing("/orgs")}
                      | {u["org_id"] for u in self.listing("/users") if u.get("org_id")})
        for o in orgs:
            org = self.call("GET", f"/orgs/{o}") or {}
            if org.get("id") != o or not org.get("name"):
                raise Refused(f"Woodpecker names organization {o} but does not have it; "
                              "its secrets listing would be another scope's")
        scopes = [("global", "global", "")]
        scopes += [("org", f"org {o}", f"/orgs/{o}") for o in orgs]
        scopes += [("repo", r["full_name"], f"/repos/{r['id']}") for r in repos]
        copies = []
        for scope, where, prefix in scopes:
            for s in self.listing(f"{prefix}/secrets"):
                if s["name"] == SECRET:
                    copies.append({"scope": scope, "where": where, "path": f"{prefix}/secrets/{SECRET}",
                                   "images": list(s.get("images") or []), "events": sorted(s.get("events") or [])})
        return repos, copies


def describe(copy):
    place = f"global {SECRET}" if copy["scope"] == "global" else f"{SECRET} on {copy['where']}"
    return f"{place} (images: {', '.join(copy['images']) or 'any'})"


def require_pinned(images):
    loose = [i for i in images if not PINNED.fullmatch(i)]
    if not images or loose:
        raise Refused(f"the filter must admit only {REGISTRY}/{PLUGIN_PACKAGE} pinned by digest, "
                      f"not: {', '.join(loose) or 'any image'}")


def current_filter(copies):
    """The filter of the global copy, or else the one filter the organization copies share, or else the
    repository copies'."""
    by_scope = {scope: [c for c in copies if c["scope"] == scope] for scope in ("global", "org", "repo")}
    source = by_scope["global"] or by_scope["org"] or by_scope["repo"]
    shapes = {(tuple(c["images"]), tuple(c["events"])) for c in source}
    if len(shapes) != 1:
        raise Refused("no single filter to carry over: " + "; ".join(describe(c) for c in source or copies))
    images, events = shapes.pop()
    require_pinned(images)
    return list(images), list(events)


def targets(repos, copies, names):
    by_name = {r["full_name"]: r for r in repos}
    unknown = [n for n in names if n not in by_name]
    if unknown:
        raise Refused(f"not Woodpecker repositories: {', '.join(unknown)}")
    chosen = names or [c["where"] for c in copies if c["scope"] == "repo"]
    if not chosen:
        raise Refused("no repository holds a copy yet; name the publishing repositories with --repo")
    return [by_name[n] for n in chosen]


def plan_changes(repos, copies, names):
    images, events = current_filter(copies)
    goal = targets(repos, copies, names)
    held = {c["where"] for c in copies if c["scope"] == "repo"}
    keep = {r["full_name"] for r in goal}
    writes = [("update" if r["full_name"] in held else "create", r) for r in goal]
    retire = [c for c in copies if c["scope"] != "repo" or c["where"] not in keep]
    return images, events, writes, retire


def warn_non_admins(woodpecker):
    users = sorted(u["login"] for u in woodpecker.listing("/users") if not u.get("admin"))
    if users:
        print(f"warning: Woodpecker users {', '.join(users)} are not admins; any of them with push on a "
              f"repository below can change the filter of its {SECRET} copy")


def print_plan(images, events, writes, retire):
    print(f"filter: images {', '.join(images)}; events {', '.join(events)}")
    for action, repo in writes:
        print(f"{action} {SECRET} on {repo['full_name']}")
    for copy in retire:
        print(f"delete {describe(copy)}")


def check_token(github_api, token):
    """Public facts about a token that may hold the publish credential; refuse any other kind."""
    if not CLASSIC_TOKEN.fullmatch(token):
        raise Refused("GitHub Packages accepts only a classic personal access token (ghp_ and 36 letters or digits)")
    status, headers, raw = http("GET", f"{github_api}/user", "token " + token)
    if status != 200:
        raise Refused(f"GitHub rejected the token: HTTP {status}")
    login = json.loads(raw)["login"]
    if login != PLUGIN_LOGIN:
        raise Refused(f"the token belongs to {login}; the plugin logs in as {PLUGIN_LOGIN}")
    scopes = {s.strip() for s in (headers.get("X-OAuth-Scopes") or "").split(",") if s.strip()}
    if "write:packages" not in scopes:
        raise Refused(f"the token lacks write:packages (scopes: {', '.join(sorted(scopes)) or 'none'})")
    if scopes - PACKAGE_SCOPES:
        raise Refused(f"the token also carries {', '.join(sorted(scopes - PACKAGE_SCOPES))}; "
                      "mint one with only write:packages")
    status, _, _ = http("GET", f"{github_api}/orgs/{PACKAGES_ORG}/packages?package_type=container&per_page=1",
                        "token " + token)
    if status != 200:
        raise Refused(f"{PACKAGES_ORG} refused to list its packages to the token: HTTP {status}")
    expiry = headers.get("github-authentication-token-expiration") or "never"
    return f"classic token of {login}, scopes {', '.join(sorted(scopes))}, expires {expiry}"


def prove_push(registry_url, token):
    """The registry opens an upload session only for a credential that may push; cancel the one opened."""
    basic = base64.b64encode(f"{PLUGIN_LOGIN}:{token}".encode()).decode()
    status, _, raw = http("GET", f"{registry_url}/token?service={REGISTRY}&scope=repository:{PLUGIN_PACKAGE}:pull,push",
                          "Basic " + basic)
    if status != 200:
        raise Refused(f"{REGISTRY} refused the token: HTTP {status}")
    bearer = "Bearer " + json.loads(raw)["token"]
    uploads = f"{registry_url}/v2/{PLUGIN_PACKAGE}/blobs/uploads/"
    status, headers, _ = http("POST", uploads, bearer)
    location = headers.get("Location")
    if status != 202 or not location:
        raise Refused(f"{REGISTRY} refused an upload to {PLUGIN_PACKAGE} with the token: HTTP {status}")
    session = urllib.parse.urljoin(uploads, location)
    if urllib.parse.urlsplit(session)[:2] != urllib.parse.urlsplit(uploads)[:2]:
        raise Refused(f"{REGISTRY} placed the probe upload on another origin; its token is not sent there")
    status, _, _ = http("DELETE", session, bearer)
    return f"may push to {REGISTRY}/{PLUGIN_PACKAGE} (probe upload opened; its cancel answered HTTP {status})"


def verify(woodpecker, names, images, events):
    _, copies = woodpecker.inventory()
    stored = {c["where"]: c for c in copies if c["scope"] == "repo"}
    for name in names:
        c = stored.get(name)
        if c is None or c["images"] != images or c["events"] != sorted(events):
            raise Refused(f"Woodpecker did not store the expected filter on {name}: "
                          f"{describe(c) if c else 'no copy'}; every other copy was left in place")
    return copies


def apply(woodpecker, github_api, registry_url, token_file, names):
    token = read_secret_file(token_file)
    print("token:", check_token(github_api, token))
    print("token:", prove_push(registry_url, token))
    repos, copies = woodpecker.inventory()
    images, events, writes, retire = plan_changes(repos, copies, names)
    print_plan(images, events, writes, retire)
    warn_non_admins(woodpecker)
    for action, repo in writes:
        body = {"value": token, "images": images, "events": events}
        if action == "update":
            woodpecker.call("PATCH", f"/repos/{repo['id']}/secrets/{SECRET}", body)
        else:
            woodpecker.call("POST", f"/repos/{repo['id']}/secrets", {"name": SECRET, **body})
    goal = [repo["full_name"] for _, repo in writes]
    verify(woodpecker, goal, images, events)
    for copy in retire:
        woodpecker.call("DELETE", copy["path"])
    left = [c for c in verify(woodpecker, goal, images, events) if c["where"] not in goal or c["scope"] != "repo"]
    if left:
        raise Refused("copies remain after deletion: " + "; ".join(describe(c) for c in left))
    print(f"done: {SECRET} is held by {', '.join(goal)} only")


def pin(woodpecker, add, drop):
    require_pinned([add or drop])
    _, copies = woodpecker.inventory()
    stray = [c for c in copies if c["scope"] != "repo"]
    if stray:
        raise Refused("run apply first; copies outside repositories remain: " + "; ".join(map(describe, stray)))
    if not copies:
        raise Refused(f"no repository holds {SECRET}")
    changes = []
    for c in copies:
        images = c["images"] + [add] if add and add not in c["images"] else [i for i in c["images"] if i != drop]
        require_pinned(images)
        changes.append((c, images))
    for c, images in changes:
        woodpecker.call("PATCH", c["path"], {"images": images})
        print(f"{c['where']}: images {', '.join(images)}")
    stored = {c["where"]: c["images"] for c in woodpecker.inventory()[1]}
    wrong = [c["where"] for c, images in changes if stored.get(c["where"]) != images]
    if wrong:
        raise Refused(f"Woodpecker did not store the new filter on {', '.join(wrong)}")


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "apply"):
        p = sub.add_parser(name)
        p.add_argument("--repo", action="append", default=[], metavar="OWNER/NAME")
        if name == "apply":
            p.add_argument("--token-file", required=True)
    p = sub.add_parser("pin").add_mutually_exclusive_group(required=True)
    p.add_argument("--add", metavar="IMAGE@sha256:DIGEST")
    p.add_argument("--drop", metavar="IMAGE@sha256:DIGEST")
    args = parser.parse_args(argv)
    missing = [k for k in ("WOODPECKER_SERVER", "WOODPECKER_TOKEN_FILE") if not os.environ.get(k)]
    if missing:
        print(f"refused: set {', '.join(missing)}", file=sys.stderr)
        return 1
    try:
        woodpecker = Woodpecker(os.environ["WOODPECKER_SERVER"], os.environ["WOODPECKER_TOKEN_FILE"])
        if args.command == "plan":
            repos, copies = woodpecker.inventory()
            print_plan(*plan_changes(repos, copies, args.repo))
            warn_non_admins(woodpecker)
        elif args.command == "apply":
            apply(woodpecker, os.environ.get("GITHUB_API_URL", "https://api.github.com"),
                  os.environ.get("REGISTRY_URL", f"https://{REGISTRY}"), args.token_file, args.repo)
        else:
            pin(woodpecker, args.add, args.drop)
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
