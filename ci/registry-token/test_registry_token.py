import contextlib
import copy
import io
import base64
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import registry_token

PLUGIN = "ghcr.io/flux-point-studios/materios-operator-kit@sha256:" + "d3" * 32
NEXT_PLUGIN = "ghcr.io/flux-point-studios/materios-operator-kit@sha256:" + "a1" * 32
OTHER_IMAGE = "ghcr.io/flux-point-studios/materios-gateway@sha256:" + "e5" * 32
EVENTS = ["cron", "deployment", "manual", "push", "release", "tag"]
WOODPECKER_TOKEN = "wp-admin-token"
NEW_TOKEN = "ghp_" + "N" * 36
OLD_VALUE = "ghp_" + "O" * 36
CONSUMERS = ["Org/kit", "Org/gateway", "Org/backend"]

REPOS = [
    {"id": 1, "full_name": "Org/kit", "org_id": 2},
    {"id": 2, "full_name": "Org/gateway", "org_id": 2},
    {"id": 3, "full_name": "Org/backend", "org_id": 2},
    {"id": 4, "full_name": "Org/docs", "org_id": 2},
    {"id": 5, "full_name": "user/app", "org_id": 1},
]
ORGS = {1: "user", 2: "Org"}

GITHUB_USERS = {
    NEW_TOKEN: ("realdecimalist", "read:packages, write:packages", 200),
    "ghp_" + "B" * 36: ("realdecimalist", "repo, write:packages", 200),
    "ghp_" + "R" * 36: ("realdecimalist", "read:packages", 200),
    "ghp_" + "S" * 36: ("someone-else", "write:packages", 200),
    "ghp_" + "P" * 36: ("realdecimalist", "write:packages", 403),
    "ghp_" + "D" * 36: ("realdecimalist", "write:packages", 200),
    "github_pat_" + "F" * 82: ("realdecimalist", None, 403),
}
PUSHERS = {NEW_TOKEN}


def secret(value, images, events):
    return {"value": value, "images": list(images), "events": list(events)}


class Fake:
    """In-memory Woodpecker secrets API plus the two GitHub endpoints the token check reads."""

    def __init__(self):
        self.global_secrets = {}
        self.orgs = dict(ORGS)
        self.user_orgs = {1}
        self.org_secrets = {o: {} for o in ORGS}
        self.repo_secrets = {r["id"]: {} for r in REPOS}
        self.writes = []
        self.users = [{"login": "realdecimalist", "admin": True, "org_id": 1}]
        self.stored_images = None
        self.stored_events = None
        self.ignore_deletes = False
        self.echo_rejections = None
        self.github_calls = []
        self.registry_tokens = {}
        self.uploads = {}

    def listing(self, secrets):
        return [{"name": n, "images": s["images"], "events": sorted(s["events"])} for n, s in secrets.items()]

    def woodpecker(self, method, parts, query, body):
        if parts in (["repos"], ["users"], ["orgs"]):
            # Like Woodpecker, the organization listing leaves out the organizations of users.
            listed = {"repos": REPOS, "users": self.users,
                      "orgs": [{"id": o, "name": n} for o, n in self.orgs.items() if o not in self.user_orgs]}
            page, per = int(query.get("page", ["1"])[0]), int(query.get("perPage", ["50"])[0])
            return 200, listed[parts[0]][(page - 1) * per:page * per]
        if parts[0] == "orgs" and len(parts) == 2:
            org = int(parts[1])
            return 200, {"id": org, "name": self.orgs[org]} if org in self.orgs else {"name": ""}
        if parts[0] == "secrets":
            store = self.global_secrets
            rest = parts[1:]
        elif parts[0] == "orgs" and parts[2] == "secrets":
            # Woodpecker answers an unknown organization with the secrets stored under no organization.
            store = self.org_secrets.get(int(parts[1]), self.global_secrets)
            rest = parts[3:]
        elif parts[0] == "repos" and parts[2] == "secrets":
            store = self.repo_secrets[int(parts[1])]
            rest = parts[3:]
        else:
            return 404, "not found"
        if method == "GET" and not rest:
            return 200, self.listing(store)
        if method != "GET":
            self.writes.append((method, "/" + "/".join(parts)))
        if method == "POST" and not rest:
            if self.echo_rejections is not None:
                return 422, self.echo_rejections + json.dumps(body)
            if body["name"] in store:
                return 500, "exists"
            store[body["name"]] = secret(body["value"], self.stored_images or body["images"],
                                         self.stored_events or body["events"])
            return 200, {"name": body["name"]}
        if not rest or rest[0] not in store:
            return 404, "not found"
        if method == "DELETE":
            if not self.ignore_deletes:
                del store[rest[0]]
            return 204, None
        if method == "PATCH":
            s = store[rest[0]]
            if body.get("value"):
                s["value"] = body["value"]
            if body.get("images") is not None:
                s["images"] = list(self.stored_images or body["images"])
            if body.get("events") is not None:
                s["events"] = list(self.stored_events or body["events"])
            return 200, {"name": rest[0]}
        return 405, "method"

    def github(self, parts, token):
        self.github_calls.append("/".join(parts))
        if token not in GITHUB_USERS:
            return 401, {"message": "Bad credentials"}, {}
        login, scopes, packages = GITHUB_USERS[token]
        headers = {} if scopes is None else {"X-OAuth-Scopes": scopes}
        headers["github-authentication-token-expiration"] = "2026-12-31 00:00:00 UTC"
        if parts == ["user"]:
            return 200, {"login": login}, headers
        if parts[:3] == ["orgs", "Flux-Point-Studios", "packages"]:
            return packages, ([] if packages == 200 else {"message": "no"}), headers
        return 404, {"message": "Not Found"}, headers

    def registry(self, method, parts, auth):
        """The GHCR token exchange and upload sessions: an upload opens only for a token that may push."""
        if parts == ["token"]:
            login, _, token = base64.b64decode(auth.removeprefix("Basic ")).decode().partition(":")
            if login != "realdecimalist" or token not in GITHUB_USERS:
                return 401, {"errors": [{"code": "UNAUTHORIZED"}]}, {}
            bearer = f"registry-bearer-{len(self.registry_tokens)}"
            self.registry_tokens[bearer] = token
            return 200, {"token": bearer}, {}
        token = self.registry_tokens.get(auth.removeprefix("Bearer "))
        path = "/" + "/".join(parts)
        if method == "POST" and path == "/v2/flux-point-studios/materios-operator-kit/blobs/uploads":
            if token not in PUSHERS:
                return 403, {"errors": [{"code": "DENIED"}]}, {}
            session = f"{path}/session-{len(self.uploads)}"
            self.uploads[session] = "open"
            return 202, None, {"Location": "/ghcr" + session}
        if method == "DELETE" and self.uploads.get(path) == "open" and token in PUSHERS:
            self.uploads[path] = "cancelled"
            return 204, None, {}
        return 404, {"errors": [{"code": "NOT_FOUND"}]}, {}


def serve(fake):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def handle_any(self):
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            auth = self.headers.get("Authorization", "")
            headers = {}
            if parts[:1] == ["api"]:
                if auth != "Bearer " + WOODPECKER_TOKEN:
                    status, payload = 401, "unauthorized"
                else:
                    status, payload = fake.woodpecker(self.command, parts[1:], parse_qs(url.query), body)
            elif parts[:1] == ["gh"]:
                status, payload, headers = fake.github(parts[1:], auth.removeprefix("token "))
            else:
                status, payload, headers = fake.registry(self.command, parts[1:], auth)
            data = b"" if payload is None else (json.dumps(payload) if not isinstance(payload, str) else payload).encode()
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PATCH = do_DELETE = handle_any

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class RegistryTokenTest(unittest.TestCase):
    def setUp(self):
        self.fake = Fake()
        self.server = serve(self.fake)
        base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.dir = tempfile.TemporaryDirectory()
        wp_file = os.path.join(self.dir.name, "woodpecker")
        with open(wp_file, "w") as f:
            f.write(WOODPECKER_TOKEN + "\n")
        self.wp_file = wp_file
        self.env = {"WOODPECKER_SERVER": base, "WOODPECKER_TOKEN_FILE": wp_file, "GITHUB_API_URL": base + "/gh",
                    "REGISTRY_URL": base + "/ghcr"}

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.dir.cleanup()

    def token_file(self, token):
        path = os.path.join(self.dir.name, "token")
        with open(path, "w") as f:
            f.write(token + "\n")
        return path

    def run_tool(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        saved = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = registry_token.main(list(argv))
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        text = out.getvalue() + err.getvalue()
        for value in [NEW_TOKEN, OLD_VALUE, WOODPECKER_TOKEN, *GITHUB_USERS, *self.fake.registry_tokens]:
            self.assertNotIn(value, text)
        return code, text

    def global_state(self):
        """Copies outside any repository: a global one and an organization one without an image filter."""
        self.fake.global_secrets["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        self.fake.org_secrets[1]["gchr_token"] = secret(OLD_VALUE, [], EVENTS)

    def holders(self):
        return {r["full_name"]: self.fake.repo_secrets[r["id"]]["gchr_token"]
                for r in REPOS if "gchr_token" in self.fake.repo_secrets[r["id"]]}

    def repo_args(self, names=CONSUMERS):
        return [a for n in names for a in ("--repo", n)]

    def test_plan_shows_the_move_and_writes_nothing(self):
        self.global_state()
        code, text = self.run_tool("plan", *self.repo_args())
        self.assertEqual(code, 0, text)
        for name in CONSUMERS:
            self.assertIn(f"create gchr_token on {name}", text)
        self.assertIn("delete global gchr_token", text)
        self.assertIn("delete gchr_token on org 1", text)
        self.assertIn(PLUGIN, text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_moves_the_token_to_exactly_the_named_repositories(self):
        self.global_state()
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 0, text)
        held = self.holders()
        self.assertEqual(sorted(held), sorted(CONSUMERS))
        for s in held.values():
            self.assertEqual(s["value"], NEW_TOKEN)
            self.assertEqual(s["images"], [PLUGIN])
            self.assertEqual(sorted(s["events"]), EVENTS)
        self.assertEqual(self.fake.global_secrets, {})
        self.assertEqual(self.fake.org_secrets[1], {})

    def test_apply_deletes_copies_on_organizations_that_hold_no_repository(self):
        self.global_state()
        self.fake.orgs.update({3: "retired-org", 4: "former-user"})
        self.fake.user_orgs.add(4)
        self.fake.users.append({"login": "former-user", "admin": True, "org_id": 4})
        for org in (3, 4):
            self.fake.org_secrets[org] = {"gchr_token": secret(OLD_VALUE, [PLUGIN], EVENTS)}
        code, text = self.run_tool("plan", *self.repo_args())
        self.assertEqual(code, 0, text)
        self.assertIn("delete gchr_token on org 3", text)
        self.assertIn("delete gchr_token on org 4", text)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 0, text)
        self.assertEqual((self.fake.org_secrets[3], self.fake.org_secrets[4]), ({}, {}))
        self.assertEqual(sorted(self.holders()), sorted(CONSUMERS))

    def test_apply_carries_over_the_filter_of_an_organization_copy_when_no_global_copy_exists(self):
        self.fake.org_secrets[2]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 0, text)
        held = self.holders()
        self.assertEqual(sorted(held), sorted(CONSUMERS))
        self.assertTrue(all(s["value"] == NEW_TOKEN and s["images"] == [PLUGIN] for s in held.values()))
        self.assertEqual(self.fake.org_secrets[2], {})

    def test_apply_refuses_organization_copies_that_disagree(self):
        self.fake.org_secrets[1]["gchr_token"] = secret(OLD_VALUE, [], EVENTS)
        self.fake.org_secrets[2]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("no single filter", text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_rotates_the_existing_copies_when_no_repository_is_named(self):
        for r in REPOS[:3]:
            self.fake.repo_secrets[r["id"]]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN))
        self.assertEqual(code, 0, text)
        held = self.holders()
        self.assertEqual(sorted(held), sorted(CONSUMERS))
        self.assertTrue(all(s["value"] == NEW_TOKEN and s["images"] == [PLUGIN] for s in held.values()))

    def test_apply_deletes_a_copy_on_a_repository_that_was_not_named(self):
        self.global_state()
        self.fake.repo_secrets[4]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 0, text)
        self.assertEqual(sorted(self.holders()), sorted(CONSUMERS))
        self.assertIn("delete gchr_token on Org/docs", text)

    def assert_refused_without_writes(self, token, fragment):
        self.global_state()
        before = copy.deepcopy((self.fake.global_secrets, self.fake.org_secrets, self.fake.repo_secrets))
        code, text = self.run_tool("apply", "--token-file", self.token_file(token), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn(fragment, text)
        self.assertEqual(self.fake.writes, [])
        self.assertEqual((self.fake.global_secrets, self.fake.org_secrets, self.fake.repo_secrets), before)

    def test_apply_refuses_a_fine_grained_token(self):
        self.assert_refused_without_writes("github_pat_" + "F" * 82, "classic")

    def test_apply_refuses_a_token_with_more_than_package_scopes(self):
        self.assert_refused_without_writes("ghp_" + "B" * 36, "repo")

    def test_apply_refuses_a_token_that_cannot_write_packages(self):
        self.assert_refused_without_writes("ghp_" + "R" * 36, "write:packages")

    def test_apply_refuses_a_token_of_another_user(self):
        self.assert_refused_without_writes("ghp_" + "S" * 36, "someone-else")

    def test_apply_refuses_a_token_the_organization_rejects(self):
        self.assert_refused_without_writes("ghp_" + "P" * 36, "403")

    def test_apply_refuses_an_unknown_token(self):
        self.assert_refused_without_writes("ghp_" + "U" * 36, "401")

    def test_apply_refuses_an_empty_token_file(self):
        self.assert_refused_without_writes("", "token file is empty")

    def test_apply_refuses_a_missing_token_file(self):
        self.global_state()
        code, text = self.run_tool("apply", "--token-file", os.path.join(self.dir.name, "absent"), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("cannot read", text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_refuses_an_unknown_repository(self):
        self.global_state()
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), "--repo", "Org/missing")
        self.assertEqual(code, 1, text)
        self.assertIn("Org/missing", text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_refuses_to_spread_a_filter_that_admits_an_unpinned_image(self):
        self.fake.global_secrets["gchr_token"] = secret(OLD_VALUE, [PLUGIN, "woodpeckerci/plugin-kaniko"], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("woodpeckerci/plugin-kaniko", text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_refuses_when_repository_copies_disagree(self):
        self.fake.repo_secrets[1]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        self.fake.repo_secrets[2]["gchr_token"] = secret(OLD_VALUE, [NEXT_PLUGIN], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN))
        self.assertEqual(code, 1, text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_keeps_the_global_copy_when_a_stored_filter_does_not_match(self):
        self.global_state()
        self.fake.stored_images = ["ghcr.io/other/image@sha256:" + "0" * 64]
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("gchr_token", self.fake.global_secrets)
        self.assertNotIn(("DELETE", "/secrets/gchr_token"), self.fake.writes)

    def test_apply_keeps_the_token_out_of_a_rejection_that_echoes_it(self):
        self.global_state()
        self.fake.echo_rejections = "Error inserting secret. "
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("HTTP 422", text)
        self.assertIn("gchr_token", self.fake.global_secrets)

    def test_apply_keeps_every_part_of_the_token_out_of_a_truncated_echo(self):
        self.global_state()
        start = len('{"name": "gchr_token", "value": "')
        self.fake.echo_rejections = "x" * (195 - start)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("HTTP 422", text)
        self.assertNotIn(NEW_TOKEN[:5], text)

    def test_apply_refuses_a_token_file_holding_the_token_twice(self):
        self.assert_refused_without_writes(NEW_TOKEN + "\n" + NEW_TOKEN, "one line")

    def test_apply_refuses_a_token_that_is_not_shaped_like_a_classic_token(self):
        self.assert_refused_without_writes(NEW_TOKEN + "x", "classic")
        self.assertEqual(self.fake.github_calls, [])

    def test_a_woodpecker_token_file_holding_the_token_twice_is_refused(self):
        self.global_state()
        with open(self.wp_file, "w") as f:
            f.write(WOODPECKER_TOKEN + "\n" + WOODPECKER_TOKEN + "\n")
        code, text = self.run_tool("plan", *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("one line", text)

    def test_a_request_the_http_client_rejects_is_refused_without_its_header(self):
        with self.assertRaises(registry_token.Refused) as caught:
            registry_token.http("GET", self.env["GITHUB_API_URL"] + "/user", "token first-line\nsecond-line")
        self.assertNotIn("first-line", str(caught.exception))
        self.assertNotIn("second-line", str(caught.exception))

    def test_apply_proves_the_token_can_push_and_cancels_the_probe_upload(self):
        self.global_state()
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 0, text)
        self.assertEqual(list(self.fake.uploads.values()), ["cancelled"])

    def test_apply_refuses_a_token_the_registry_will_not_let_push(self):
        self.assert_refused_without_writes("ghp_" + "D" * 36, "upload")

    def test_plan_refuses_a_repository_whose_organization_woodpecker_does_not_have(self):
        self.global_state()
        REPOS.append({"id": 6, "full_name": "gone/app", "org_id": 7})
        self.fake.repo_secrets[6] = {}
        try:
            code, text = self.run_tool("plan", *self.repo_args())
            self.assertEqual(code, 1, text)
            self.assertIn("organization 7", text)
            code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
            self.assertEqual(code, 1, text)
            self.assertEqual(self.fake.writes, [])
            self.assertIn("gchr_token", self.fake.global_secrets)
        finally:
            REPOS.pop()

    def test_apply_refuses_to_spread_a_filter_that_admits_another_image_by_digest(self):
        self.fake.global_secrets["gchr_token"] = secret(OLD_VALUE, [PLUGIN, OTHER_IMAGE], EVENTS)
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn(OTHER_IMAGE, text)
        self.assertEqual(self.fake.writes, [])

    def test_apply_keeps_the_global_copy_when_a_stored_event_filter_does_not_match(self):
        self.global_state()
        self.fake.stored_events = ["pull_request", "push"]
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("did not store the expected filter", text)
        self.assertIn("gchr_token", self.fake.global_secrets)
        self.assertFalse([w for w in self.fake.writes if w[0] == "DELETE"])

    def test_apply_refuses_when_a_deleted_copy_is_still_listed(self):
        self.global_state()
        self.fake.ignore_deletes = True
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertIn("copies remain after deletion", text)
        self.assertIn("global gchr_token", text)
        self.assertNotIn("done:", text)

    def test_pin_refuses_when_woodpecker_does_not_store_the_new_filter(self):
        for r in REPOS[:3]:
            self.fake.repo_secrets[r["id"]]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        self.fake.stored_images = [PLUGIN]
        code, text = self.run_tool("pin", "--add", NEXT_PLUGIN)
        self.assertEqual(code, 1, text)
        self.assertIn("did not store the new filter", text)

    def test_plan_names_every_woodpecker_user_who_is_not_an_admin(self):
        self.global_state()
        self.fake.users += [{"login": "collaborator", "admin": False}, {"login": "second-admin", "admin": True}]
        code, text = self.run_tool("plan", *self.repo_args())
        self.assertEqual(code, 0, text)
        warnings = [line for line in text.splitlines() if line.startswith("warning:")]
        self.assertEqual(len(warnings), 1, text)
        self.assertIn("collaborator", warnings[0])
        self.assertNotIn("second-admin", warnings[0])
        self.assertIn("push", warnings[0])

    def test_apply_names_every_woodpecker_user_who_is_not_an_admin_before_it_writes(self):
        self.global_state()
        self.fake.users.append({"login": "collaborator", "admin": False})
        self.fake.echo_rejections = "Error inserting secret. "
        code, text = self.run_tool("apply", "--token-file", self.token_file(NEW_TOKEN), *self.repo_args())
        self.assertEqual(code, 1, text)
        self.assertEqual([w[0] for w in self.fake.writes], ["POST"], text)
        warnings = [line for line in text.splitlines() if line.startswith("warning:")]
        self.assertEqual(len(warnings), 1, text)
        self.assertIn("collaborator", warnings[0])

    def test_plan_reads_every_page_of_woodpecker_users(self):
        self.global_state()
        self.fake.users += [{"login": f"admin-{i}", "admin": True} for i in range(60)]
        self.fake.users.append({"login": "last-page-user", "admin": False})
        code, text = self.run_tool("plan", *self.repo_args())
        self.assertEqual(code, 0, text)
        self.assertIn("last-page-user", text)

    def test_plan_with_only_admins_prints_no_warning(self):
        self.global_state()
        code, text = self.run_tool("plan", *self.repo_args())
        self.assertEqual(code, 0, text)
        self.assertNotIn("warning:", text)

    def test_pin_adds_and_drops_a_plugin_digest_on_every_copy(self):
        for r in REPOS[:3]:
            self.fake.repo_secrets[r["id"]]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("pin", "--add", NEXT_PLUGIN)
        self.assertEqual(code, 0, text)
        self.assertTrue(all(s["images"] == [PLUGIN, NEXT_PLUGIN] and s["value"] == OLD_VALUE
                            for s in self.holders().values()))
        code, text = self.run_tool("pin", "--drop", PLUGIN)
        self.assertEqual(code, 0, text)
        self.assertTrue(all(s["images"] == [NEXT_PLUGIN] for s in self.holders().values()))

    def test_pin_refuses_to_drop_the_last_digest(self):
        for r in REPOS[:3]:
            self.fake.repo_secrets[r["id"]]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("pin", "--drop", PLUGIN)
        self.assertEqual(code, 1, text)
        self.assertEqual(self.fake.writes, [])

    def test_pin_refuses_an_unpinned_image(self):
        for r in REPOS[:3]:
            self.fake.repo_secrets[r["id"]]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("pin", "--add", "ghcr.io/flux-point-studios/materios-operator-kit:ci-oci-index")
        self.assertEqual(code, 1, text)
        self.assertEqual(self.fake.writes, [])

    def test_pin_refuses_a_digest_of_an_image_that_is_not_the_plugin(self):
        for r in REPOS[:3]:
            self.fake.repo_secrets[r["id"]]["gchr_token"] = secret(OLD_VALUE, [PLUGIN], EVENTS)
        code, text = self.run_tool("pin", "--add", "docker.io/library/alpine@sha256:" + "ee" * 32)
        self.assertEqual(code, 1, text)
        self.assertEqual(self.fake.writes, [])

    def test_pin_refuses_while_a_global_copy_remains(self):
        self.global_state()
        code, text = self.run_tool("pin", "--add", NEXT_PLUGIN)
        self.assertEqual(code, 1, text)
        self.assertIn("apply", text)
        self.assertEqual(self.fake.writes, [])


if __name__ == "__main__":
    unittest.main()
