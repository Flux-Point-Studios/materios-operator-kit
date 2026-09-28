import os
import re
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")


def read(path):
    with open(os.path.join(ROOT, path)) as f:
        return f.read()


def anchors(pipeline):
    """Name -> body lines of every mapping anchored under variables."""
    head = pipeline.split("\nsteps:\n", 1)[0]
    return {m.group(1): m.group(2) for m in re.finditer(r"^  - &([a-z_]+)\n((?:    .*\n)+)", head, flags=re.M)}


def mapping(body, indent, named):
    """Key -> scalar text of a mapping at that indent, with every `<<: *anchor` merge resolved as YAML
    does: a key the mapping sets itself wins over a merged one."""
    merged, own = {}, {}
    for key, value in re.findall(rf"^{indent}([a-z_<]+):(.*)$", body, flags=re.M):
        if key == "<<":
            merged.update(mapping(named[value.strip().lstrip("*")], "    ", named))
        else:
            own[key] = value.strip()
    return {**merged, **own}


def steps(pipeline):
    """Name -> (image, declared settings, step text), for every step."""
    named = anchors(pipeline)
    out = {}
    for block in re.split(r"^  - name: ", pipeline.split("\nsteps:\n", 1)[1], flags=re.M)[1:]:
        name, body = block.split("\n", 1)
        image = re.search(r"^    image: (.*)$", body, flags=re.M).group(1).strip()
        settings = re.search(r"^    settings:\n((?:      .*\n|        .*\n)+)", body, flags=re.M)
        out[name.strip()] = (image, mapping(settings.group(1), "      ", named) if settings else {}, body)
    return out


def kaniko_inputs():
    lines = read("ci/registry-token/kaniko-plugin-inputs").splitlines()
    return lines[0].split()[-1], {line for line in lines if line and not line.startswith("#")}


PIPELINE = read(".woodpecker.yaml")
KANIKO = re.search(r"^  - &kaniko (\S+)$", PIPELINE, flags=re.M).group(1)
STEPS = steps(PIPELINE)
TOKEN_STEPS = {n: s for n, s in STEPS.items() if "from_secret: gchr_token" in s[2]}
KANIKO_STEPS = {n: s for n, s in STEPS.items() if s[0] == "*kaniko" and "\n    commands:" not in s[2]}


class PipelineInputsTest(unittest.TestCase):
    # Woodpecker fills a plugin input a step leaves undeclared from the variables of a manual run or
    # a restart, so every input of a step that publishes, or builds what is published, is declared.

    def test_every_step_holding_the_token_declares_every_plugin_input(self):
        inputs = {k.lower() for k in re.findall(r"\bPLUGIN_([A-Z_]+)", read("ci/oci-index/oci-index.sh"))}
        self.assertEqual(sorted(TOKEN_STEPS), ["publish-index", "push-npm-publish", "push-oci-index"])
        for name, (_, declared, _) in TOKEN_STEPS.items():
            self.assertEqual(inputs - set(declared), set(), f"{name} leaves plugin inputs undeclared")

    def test_the_kaniko_input_list_is_the_one_of_the_pinned_plugin(self):
        self.assertEqual(kaniko_inputs()[0], KANIKO)

    def test_every_kaniko_step_declares_every_kaniko_input(self):
        _, inputs = kaniko_inputs()
        self.assertEqual(sorted(KANIKO_STEPS), ["build-amd64", "build-arm64", "build-npm-publish", "build-oci-index"])
        for name, (_, declared, _) in KANIKO_STEPS.items():
            self.assertEqual(inputs - set(declared), set(), f"{name} leaves kaniko inputs undeclared")

    def test_every_layout_the_token_publishes_is_built_by_a_kaniko_step(self):
        built = {p for _, declared, _ in KANIKO_STEPS.values()
                 for p in re.findall(r"--oci-layout-path=(\S+)", declared.get("extra_opts", ""))}
        published = {p for _, _, body in TOKEN_STEPS.values() for p in re.findall(r"^        - (/\S+)=", body, flags=re.M)}
        self.assertEqual(sorted(published), ["/woodpecker/oci/amd64", "/woodpecker/oci/arm64", "/woodpecker/oci/npm-publish",
                                             "/woodpecker/oci/oci-index"])
        self.assertEqual(published - built, set())

    def test_every_image_a_kaniko_step_builds_on_is_pinned_by_digest(self):
        for name, (_, declared, _) in KANIKO_STEPS.items():
            path = declared.get("dockerfile") or "Dockerfile"
            bases = re.findall(r"^FROM (\S+)|^COPY --from=(\S+)", read(path), flags=re.M)
            refs = [a or b for a, b in bases]
            self.assertTrue(refs, f"{path} names no base image")
            for ref in refs:
                self.assertRegex(ref, r"@sha256:[0-9a-f]{64}$", f"{name} builds {path} on {ref}, not pinned by digest")


if __name__ == "__main__":
    unittest.main()
