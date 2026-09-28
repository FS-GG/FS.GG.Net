#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import pathlib
import unittest
from unittest.mock import patch


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "v2_ci_ordinary_observe", ROOT / "tools/v2-ci-ordinary-observe.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class NetObservationSourceTests(unittest.TestCase):
    def test_tools_match_recorded_repaired_source_digests(self):
        policy = json.loads((ROOT / "policy/v2-ci-ordinary-settlement.json").read_text())
        expected = {
            "tools/v2-ci-ordinary-observe.py": policy["sourceImplementation"]["observerSha256"],
            "tools/v2-ci-ordinary-qualification.py":
                policy["sourceImplementation"]["qualificationSha256"],
        }
        for relative, digest in expected.items():
            self.assertEqual(digest, hashlib.sha256((ROOT / relative).read_bytes()).hexdigest())

    def test_net_profile_is_fixed_to_exact_checks_and_producers(self):
        policy = json.loads((ROOT / "policy/v2-ci-ordinary-settlement.json").read_text())
        profile = MODULE.QUALIFICATION.source_profile(policy, "net-v1")
        self.assertEqual("FS-GG/FS.GG.Net", profile["repository"])
        self.assertEqual(1305845505, profile["repositoryId"])
        self.assertEqual(15368, profile["requiredCheckAppId"])
        self.assertEqual({'contract-coherence / coherence', 'Build + test (locked restore)'},
                         set(profile["requiredChecks"]))
        self.assertEqual(set(policy["qualification"]["requiredGateChecks"]),
                         set(profile["requiredGateChecks"]))
        self.assertEqual(profile["requiredChecks"], policy["qualification"]["requiredChecks"])
        self.assertEqual(profile["checkProducers"], policy["qualification"]["checkProducers"])
        self.assertEqual(
            {
                "Build + test (locked restore)": 316245439,
                "kit / coordination-kit": 316890379,
                "contract-coherence / coherence": 316245439,
                "materialize / receiver-validate": 316890380,
            },
            {name: producer["workflowId"]
             for name, producer in profile["checkProducers"].items()},
        )
        with patch.object(MODULE.QUALIFICATION, "read_json", return_value=policy):
            with self.assertRaisesRegex(MODULE.QUALIFICATION.Refusal, "no rehearsal activation"):
                MODULE.observe({"FSGG_V2_SOURCE_PROFILE": "net-v1"}, rehearsal=True)

    def test_net_local_policy_is_admitted_before_runtime_event_fences(self):
        policy = json.loads((ROOT / "policy/v2-ci-ordinary-settlement.json").read_text())
        env = {
            "FSGG_V2_SOURCE_PROFILE": "net-v1",
            "GITHUB_SHA": "a" * 40,
            "GITHUB_REPOSITORY": "FS-GG/FS.GG.Net",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/heads/main",
        }
        repository = {
            "id": 1305845505,
            "full_name": "FS-GG/FS.GG.Net",
            "default_branch": "main",
        }
        with patch.object(MODULE.QUALIFICATION, "read_json", return_value=policy), \
                patch.object(MODULE, "api", return_value=repository):
            with self.assertRaisesRegex(MODULE.QUALIFICATION.Refusal,
                                        "pinned protected-main event"):
                MODULE.observe(env)

    def test_current_authority_reads_net_policy_and_workflow_from_one_main_revision(self):
        policy = json.loads((ROOT / "policy/v2-ci-ordinary-settlement.json").read_text())
        revision = "a" * 40

        def api(path):
            if path == "repos/FS-GG/FS.GG.Net/git/ref/heads/main":
                return {"object": {"sha": revision}}
            prefix = "repos/FS-GG/FS.GG.Net/contents/"
            if path.startswith(prefix) and path.endswith("?ref=" + revision):
                relative = path[len(prefix):].split("?ref=", 1)[0]
                return {
                    "encoding": "base64",
                    "content": base64.b64encode((ROOT / relative).read_bytes()).decode(),
                }
            raise AssertionError(path)

        with patch.object(MODULE, "api", side_effect=api):
            MODULE.current_authority("FS-GG/FS.GG.Net", policy)

    def test_workflow_prepares_bounded_settlement_with_exact_receipt_guard(self):
        workflow = (ROOT / ".github/workflows/v2-ci-ordinary-settlement.yml").read_text()
        self.assertIn("  push:\n    branches: [main]", workflow)
        self.assertNotIn("    if: ${{ false }}", workflow)
        self.assertIn("if: needs.preflight.outputs.activation == 'true'", workflow)
        self.assertIn("environment: ordinary-v2", workflow)
        self.assertIn("FSGG_V2_SOURCE_PROFILE: net-v1", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn("python3 tools/v2-ci-ordinary-observe.py produce", workflow)
        self.assertIn("python3 tools/v2-ci-ordinary-observe.py verify", workflow)
        self.assertIn("PACKAGE_VERSION: 0.1.5", workflow)
        self.assertIn("PACKAGE_SHA256: 3567a92825917a7d537f6c5c545d3a7947bc35edd666fc3a1898de3bf97267c9", workflow)
        self.assertIn("https://github.com/FS-GG/FS.GG.Coordination/releases/download/v$PACKAGE_VERSION/FS.GG.Coordination.Cli.$PACKAGE_VERSION.nupkg", workflow)
        self.assertNotIn("api.nuget.org/v3-flatcontainer", workflow)
        self.assertIn("ordinary-settlement execute", workflow)
        for name in ("V2_ORDINARY_APP_ID", "V2_ORDINARY_APP_PRIVATE_KEY", "V2_ORDINARY_AUTHORIZER_PRIVATE_KEY"):
            self.assertIn("${{ secrets." + name + " }}", workflow)
        for forbidden in ("workflow_dispatch:", "repository_dispatch:", "pull_request:", "pull_request_target:", "V1_ADMISSION", "CALLABLE_ISOLATED_OPERATION"):
            self.assertNotIn(forbidden, workflow)

    def test_policy_anchor_environment_and_published_package_are_bounded(self):
        policy = json.loads((ROOT / "policy/v2-ci-ordinary-settlement.json").read_text())
        anchor = json.loads((ROOT / "policy/v2-ci-ordinary-settlement-anchor.json").read_text())
        self.assertEqual("v2-ci-i1-ordinary-settlement-v1", policy["policyId"])
        self.assertEqual(policy["policyId"], anchor["policyId"])
        self.assertEqual("installed", policy["status"])
        self.assertTrue(policy["credentialJob"]["installed"])
        observation = policy["credentialJob"]["liveObservation"]
        self.assertEqual(22920188172, observation["environmentId"])
        self.assertEqual(61287584, observation["branchPolicyId"])
        self.assertEqual("main", observation["customBranchPolicy"])
        self.assertEqual(3, observation["secretCount"])
        self.assertEqual({
            "V2_ORDINARY_APP_ID",
            "V2_ORDINARY_APP_PRIVATE_KEY",
            "V2_ORDINARY_AUTHORIZER_PRIVATE_KEY",
        }, set(observation["secretNames"]))
        self.assertEqual("published-verified",
                         policy["packagePin"]["status"])
        self.assertEqual("0.1.5", policy["packagePin"]["version"])
        self.assertEqual("3567a92825917a7d537f6c5c545d3a7947bc35edd666fc3a1898de3bf97267c9",
                         policy["packagePin"]["sha256"])
        self.assertTrue(policy["packagePin"]["servedPackageVerified"])
        self.assertEqual(3, len(policy["credentialInventory"]))
        self.assertTrue(all(item["provisioned"] for item in policy["credentialInventory"]))
        self.assertEqual(5064713, anchor["writer"]["appId"])
        self.assertEqual(164553252, anchor["writer"]["installationId"])
        self.assertEqual(1351660651, anchor["writer"]["repositoryId"])


if __name__ == "__main__":
    unittest.main()
