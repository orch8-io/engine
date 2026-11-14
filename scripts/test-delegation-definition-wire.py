"""Cross-language checks of captured exact-byte definition and input vectors."""
import base64
import hashlib
import json
from pathlib import Path
import unittest

FIXTURE = json.loads((Path(__file__).resolve().parents[1] / "contracts/vectors/delegation-definition-v1.json").read_text())
ENVELOPE = FIXTURE["envelope"]
PAYLOAD = base64.b64decode(ENVELOPE["payload_base64"], validate=True)


class DefinitionWireVector(unittest.TestCase):
    def test_exact_bytes_and_full_width_epoch(self):
        self.assertEqual(len(PAYLOAD), ENVELOPE["reference"]["payload_bytes"])
        self.assertEqual(hashlib.sha256(PAYLOAD).hexdigest(), ENVELOPE["reference"]["payload_sha256"])
        self.assertIsInstance(ENVELOPE["scope"]["claim_epoch"], str)
        self.assertEqual(int(ENVELOPE["scope"]["claim_epoch"]), (1 << 64) - 1)
        params = json.loads(PAYLOAD)["definitions"][0]["definition"]["blocks"][0]["params"]
        self.assertEqual(params["max_u64"], (1 << 64) - 1)
        self.assertEqual(params["over53"], 9007199254740993)
        self.assertEqual(params["nul"], "a\0b")
        self.assertEqual(params["\ue000"], "BMP")
        self.assertEqual(params["\U00010000"], "non-BMP")

    def test_python_json_reserialization_is_not_the_wire_identity(self):
        rewritten = json.dumps(json.loads(PAYLOAD), ensure_ascii=False, separators=(",", ":")).encode()
        self.assertNotEqual(hashlib.sha256(rewritten).hexdigest(), ENVELOPE["reference"]["payload_sha256"])
        changed = PAYLOAD[:-1] + bytes([PAYLOAD[-1] ^ 1])
        self.assertNotEqual(hashlib.sha256(changed).hexdigest(), ENVELOPE["reference"]["payload_sha256"])


class InputWireVector(unittest.TestCase):
    def test_input_byte_identity_expansion_count_and_full_epoch(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] / "contracts/vectors/delegation-input-v1.json").read_text())
        envelope = fixture["envelope"]
        payload = base64.b64decode(envelope["payload_base64"], validate=True)
        reference = fixture["reference"]
        self.assertEqual(envelope["reference"], reference)
        self.assertEqual(hashlib.sha256(payload).hexdigest(), reference["payload_sha256"])
        self.assertEqual(len(payload), reference["payload_bytes"])
        def cost(value):
            if isinstance(value, dict):
                return 64 + sum(64 + len(k.encode()) + cost(v) for k, v in value.items())
            if isinstance(value, list):
                return 64 + sum(cost(v) for v in value)
            return 64 + (len(value.encode()) if isinstance(value, str) else 0)
        value = json.loads(payload)
        self.assertEqual(cost(value), reference["expanded_bytes"])
        self.assertEqual(value["large"], (1 << 64) - 1)
        self.assertEqual(envelope["scope"]["claim_epoch"], str((1 << 64) - 1))
        self.assertEqual(value["private"], "INPUT_PRIVATE_CANARY 😺\0")

    def test_input_reserialization_is_not_its_authenticated_wire_identity(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] / "contracts/vectors/delegation-input-v1.json").read_text())
        payload = base64.b64decode(fixture["envelope"]["payload_base64"], validate=True)
        rewritten = json.dumps(json.loads(payload), ensure_ascii=False, separators=(",", ":")).encode()
        self.assertNotEqual(hashlib.sha256(rewritten).hexdigest(), fixture["reference"]["payload_sha256"])


if __name__ == "__main__":
    unittest.main()
