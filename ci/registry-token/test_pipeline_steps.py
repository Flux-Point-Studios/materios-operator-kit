import os
import re
import unittest

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")


def read(path):
    with open(os.path.join(ROOT, path)) as f:
        return f.read()


def token_steps(pipeline):
    """Name -> declared settings keys, for every step that is handed gchr_token."""
    steps = {}
    for block in re.split(r"^  - name: ", pipeline.split("\nsteps:\n", 1)[1], flags=re.M)[1:]:
        if "from_secret: gchr_token" not in block:
            continue
        name, body = block.split("\n", 1)
        settings = re.search(r"^    settings:\n((?:      .*\n|        .*\n)+)", body, flags=re.M)
        steps[name.strip()] = set(re.findall(r"^      ([a-z_]+):", settings.group(1), flags=re.M))
    return steps


class PipelineTokenStepsTest(unittest.TestCase):
    def test_every_step_holding_the_token_declares_every_plugin_input(self):
        # Woodpecker fills a plugin input a step leaves undeclared from the variables of a manual run
        # or a restart, and push-oci-index's target shares the operator image's repository.
        inputs = {k.lower() for k in re.findall(r"\bPLUGIN_([A-Z_]+)", read("ci/oci-index/oci-index.sh"))}
        steps = token_steps(read(".woodpecker.yaml"))
        self.assertEqual(sorted(steps), ["publish-index", "push-oci-index"])
        for name, declared in steps.items():
            self.assertEqual(inputs - declared, set(), f"{name} leaves plugin inputs undeclared")


if __name__ == "__main__":
    unittest.main()
