"""Ed25519 纯 Python 实现：RFC 8032 官方测试向量 + 与 OpenSSL 无关的自洽性。"""
import unittest

from pv_service import ed25519 as ed

# RFC 8032 §7.1 TEST 1 / TEST 2（空消息与单字节消息，覆盖签名与验证全路径）
VECTOR_1 = (
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
    "",
    "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"
    "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
)
VECTOR_2 = (
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
    "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c",
    "72",
    "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da"
    "085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00",
)


class TestEd25519(unittest.TestCase):
    def _check_vector(self, sh, ph, mh, sigh):
        seed = bytes.fromhex(sh)
        pk = ed.publickey(seed)
        self.assertEqual(pk.hex(), ph)
        msg = bytes.fromhex(mh)
        sig = ed.signature(msg, seed, pk)
        self.assertEqual(sig.hex(), sigh)
        self.assertTrue(ed.verify(pk, msg, sig))
        return pk, msg, sig

    def test_rfc8032_vector_1_empty_message(self):
        self._check_vector(*VECTOR_1)

    def test_rfc8032_vector_2_one_byte(self):
        self._check_vector(*VECTOR_2)

    def test_deterministic_signature(self):
        seed = bytes.fromhex(VECTOR_1[0])
        pk = ed.publickey(seed)
        s1 = ed.signature(b"deterministic", seed, pk)
        s2 = ed.signature(b"deterministic", seed, pk)
        self.assertEqual(s1, s2)

    def test_tamper_message_or_signature_fails(self):
        pk, msg, sig = self._check_vector(*VECTOR_2)
        self.assertFalse(ed.verify(pk, msg + b"\x00", sig))
        bad = bytearray(sig)
        bad[63] ^= 0x01
        self.assertFalse(ed.verify(pk, msg, bytes(bad)))
        bad = bytearray(sig)
        bad[0] ^= 0x01
        self.assertFalse(ed.verify(pk, msg, bytes(bad)))

    def test_wrong_key_rejects(self):
        pk1, msg, sig1 = self._check_vector(*VECTOR_1)
        pk2 = ed.publickey(bytes.fromhex(VECTOR_2[0]))
        self.assertFalse(ed.verify(pk2, msg, sig1))

    def test_malformed_inputs_return_false(self):
        self.assertFalse(ed.verify(b"short", b"m", b"\x00" * 64))
        self.assertFalse(ed.verify(b"\x00" * 32, b"m", b"short"))
        # 全零公钥点不在曲线上
        self.assertFalse(ed.verify(b"\x00" * 32, b"m", b"\x00" * 64))

    def test_keypair_roundtrip_long_message(self):
        seed, pk = ed.generate_keypair()
        self.assertEqual(len(seed), 32)
        self.assertEqual(len(pk), 32)
        msg = b"evidence package " * 1000
        sig = ed.signature(msg, seed, pk)
        self.assertTrue(ed.verify(pk, msg, sig))
        self.assertFalse(ed.verify(pk, msg[:-1], sig))

    def test_base64_helpers(self):
        self.assertEqual(ed.b64d(ed.b64e(b"\x00\xff")), b"\x00\xff")


if __name__ == "__main__":
    unittest.main()
