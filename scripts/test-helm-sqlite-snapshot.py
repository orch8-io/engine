#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


def kubectl(argv):
    fixture = Path(os.environ["ORCH8_TEST_SNAPSHOT_STATE"])
    state = json.loads(fixture.read_text())
    command = argv[3:]
    kind = command[:2]
    snapshot = state.get("snapshot")
    if kind == ["get", "deployment"]:
        print(state["revision"], "uid", state["generation"], state["generation"],
              state["replicas"], state["owner"], state["fenced_generation"],
              state["fenced_claim_uid"], state["last"])
    elif kind == ["get", "pvc"]:
        assert command[2] == "data"
        if state["claim_phase"] == "Bound":
            print(state["claim_uid"])
    elif kind == ["get", "volumesnapshot"]:
        if snapshot:
            print("data snapshots uid", snapshot["generation"], snapshot["claim_uid"], snapshot["phase"])
    elif kind == ["get", "pods"]:
        if state["mounted_checks"]:
            print("sqlite-pod data")
            state["mounted_checks"] -= 1
            state["events"].append("mounted")
        else:
            print("unrelated-pod other-claim")
            state["events"].append("unmounted")
    elif kind == ["patch", "deployment"]:
        patch = json.loads(command[-1])
        assert patch["metadata"]["resourceVersion"] == str(state["revision"])
        annotations = patch["metadata"]["annotations"]
        replicas = patch["spec"]["replicas"]
        if replicas == 0:
            assert state["replicas"] == 1
            state["owner"] = annotations["orch8.io/sqlite-snapshot"]
            state["fenced_generation"] = annotations["orch8.io/sqlite-snapshot-generation"]
            state["fenced_claim_uid"] = annotations["orch8.io/sqlite-snapshot-claim-uid"]
            if state["replace_on_fence"]:
                state["claim_uid"] = "replacement"
            state["events"].append("fenced")
        else:
            assert snapshot["phase"] == "ready" and state["replicas"] == 0
            assert annotations["orch8.io/sqlite-snapshot-claim-uid"] is None
            state["owner"] = state["fenced_generation"] = state["fenced_claim_uid"] = "-"
            state["last"] = annotations["orch8.io/sqlite-last-snapshot"]
            state["events"].append("resumed")
        state["replicas"] = replicas
        state["generation"] += 1
        state["revision"] += 1
    elif kind == ["create", "-f"]:
        manifest = sys.stdin.read()
        assert "persistentVolumeClaimName: data" in manifest
        assert "volumeSnapshotClassName: snapshots" in manifest
        assert f'orch8.io/sqlite-snapshot-claim-uid: "{state["claim_uid"]}"' in manifest
        assert state["events"][-1] == "unmounted" and state["replicas"] == 0
        assert snapshot is None
        state["snapshot"] = {
            "generation": state["generation"], "phase": state["create_phase"],
            "claim_uid": state["claim_uid"],
        }
        state["events"].append("created")
        fixture.write_text(json.dumps(state))
        return state["interrupt"]
    else:
        raise AssertionError(command)
    fixture.write_text(json.dumps(state))
    return 0


class SnapshotJobTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        scratch = Path(folder.name)
        self.fixture = scratch / "state.json"
        for name, content in [
            ("kubectl", f'exec "{sys.executable}" "{Path(__file__).resolve()}" --kubectl "$@"'),
            ("sleep", "exit 0"),
        ]:
            script = scratch / name
            script.write_text(f"#!/bin/sh\n{content}\n")
            script.chmod(0o700)
        self.env = {
            **os.environ,
            "PATH": f"{scratch}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(scratch),
            "ORCH8_TEST_SNAPSHOT_STATE": str(self.fixture),
            "POD_NAMESPACE": "fixture",
            "DEPLOYMENT": "engine",
            "CLAIM": "data",
            "SNAPSHOT": "backup",
            "SNAPSHOT_CLASS": "snapshots",
            "TIMEOUT_SECONDS": "60",
        }
        self.state = {
            "revision": 5, "generation": 1, "replicas": 1, "owner": "-",
            "fenced_generation": "-", "last": "-", "events": [],
            "mounted_checks": 1, "create_phase": "ready", "interrupt": 0,
            "claim_uid": "original", "fenced_claim_uid": "-",
            "replace_on_fence": False, "claim_phase": "Bound",
        }

    def run_job(self):
        script = Path(__file__).resolve().parents[1] / "deploy/helm/orch8/files/sqlite-snapshot.sh"
        self.fixture.write_text(json.dumps(self.state))
        result = subprocess.run(["sh", script], env=self.env, capture_output=True, text=True, timeout=8)
        self.state = json.loads(self.fixture.read_text())
        return result

    def test_fences_waits_for_unmount_and_resumes_once(self):
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        kubeconfig = self.fixture.with_name("snapshot.kubeconfig")
        self.assertEqual(kubeconfig.stat().st_mode & 0o777, 0o600)
        self.assertIn("server: https://kubernetes.default.svc", kubeconfig.read_text())
        self.assertIn("tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token", kubeconfig.read_text())
        self.assertEqual(self.state["events"], [
            "fenced", "mounted", "unmounted", "created", "unmounted", "resumed",
        ])
        self.assertEqual(self.run_job().returncode, 0)
        self.assertEqual(self.state["events"].count("resumed"), 1)

    def test_interrupted_create_recovers_from_persisted_fence_and_snapshot(self):
        self.state["interrupt"] = 23
        self.assertEqual(self.run_job().returncode, 23)
        self.assertEqual(self.state["replicas"], 0)
        result = self.run_job()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state["events"].count("created"), 1)
        self.assertEqual(self.state["replicas"], 1)

    def test_changed_generation_cannot_resume_from_a_stale_job(self):
        self.state["interrupt"] = 23
        self.assertEqual(self.run_job().returncode, 23)
        self.state["generation"] += 1
        result = self.run_job()
        self.assertEqual(result.returncode, 1)
        self.assertIn("Deployment changed", result.stderr)
        self.assertEqual(self.state["replicas"], 0)

    def test_csi_failure_leaves_the_writer_fenced(self):
        self.state["create_phase"] = "error"
        result = self.run_job()
        self.assertEqual(result.returncode, 1)
        self.assertIn("CSI snapshot failed", result.stderr)
        self.assertEqual(self.state["replicas"], 0)

    def test_same_name_claim_replacement_before_creation_prevents_snapshot(self):
        self.state["replace_on_fence"] = True
        result = self.run_job()
        self.assertEqual(result.returncode, 1)
        self.assertIn("SQLite claim was replaced", result.stderr)
        self.assertNotIn("snapshot", self.state)
        self.assertEqual(self.state["replicas"], 0)

    def test_same_name_claim_replacement_after_creation_prevents_resume(self):
        self.state["interrupt"] = 23
        self.assertEqual(self.run_job().returncode, 23)
        self.state["claim_uid"] = "replacement"
        result = self.run_job()
        self.assertEqual(result.returncode, 1)
        self.assertIn("SQLite claim was replaced", result.stderr)
        self.assertEqual(self.state["replicas"], 0)

    def test_unbound_claim_cannot_withdraw_the_running_writer(self):
        self.state["claim_phase"] = "Pending"
        result = self.run_job()
        self.assertEqual(result.returncode, 1)
        self.assertIn("SQLite claim is unbound", result.stderr)
        self.assertEqual(self.state["replicas"], 1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--kubectl":
        sys.exit(kubectl(sys.argv[2:]))
    unittest.main()
