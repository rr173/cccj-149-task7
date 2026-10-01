"""离线核验：篡改/缺文件/坏链/坏签名逐项失败，且绝不标有效。"""
import io
import json
import os
import subprocess
import sys
import tarfile

from tests.test_evidence_export import ExportTestBase
from pv_service import package_codec as codec
from pv_service.verifier import verify_package


def _tar_bytes(names: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, data in names.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o444
            info.uid = info.gid = 0
            info.mtime = 0
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _unpack(data: bytes) -> dict[str, bytes]:
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tar:
        return {m.name: tar.extractfile(m).read() for m in tar.getmembers()}


def _repack(files: dict[str, bytes]) -> bytes:
    return _tar_bytes(files)


class TestOfflineVerification(ExportTestBase):
    def setUp(self):
        super().setUp()
        r = self.app.cases.intake("HOSPITAL", "H-VER-1", self.zhang_patient())
        self.cid = r["case_id"]
        self.pump()
        self.clock.advance(days=1)
        self.app.cases.add_followup(self.cid, {"outcome": "好转"})
        self.pump()
        t = self.exports.request_export(self.cid)
        self.exports.run_until_idle()
        self.task = self.exports.get_task(t["task_id"])
        self.good = self.exports.package_path(t["task_id"])[0].read_bytes()

    def test_valid_package_passes_all_checks(self):
        rep = verify_package(self.good)
        self.assertTrue(rep["valid"], rep["errors"])
        codes = {c["code"]: c for c in rep["checks"]}
        for code in ("PKG_OPEN", "PKG_MANIFEST", "SIGNATURE", "MANIFEST_ROOT",
                     "FILE_PRESENT", "FILE_DIGEST", "TIMELINE_CHAIN",
                     "SNAPSHOT_BINDING", "VERSION_STATUS", "REPORTS", "RELATIONS"):
            self.assertTrue(codes[code]["ok"], f"{code}: {codes[code]['message']}")

    # -- 缺文件 -------------------------------------------------------------
    def test_missing_file_is_reported_and_invalid(self):
        files = _unpack(self.good)
        victim = next(n for n in files if n.startswith("sources/"))
        del files[victim]
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        file_check = next(c for c in rep["checks"] if c["code"] == "FILE_PRESENT")
        self.assertFalse(file_check["ok"])
        self.assertTrue(any(victim in d for d in file_check["details"]))
        self.assertTrue(any("FILE_PRESENT" in e for e in rep["errors"]))

    # -- 摘要篡改（改文件但不改清单）---------------------------------------
    def test_tampered_file_digest_detected(self):
        files = _unpack(self.good)
        target = next(n for n in files if n.startswith("export/snapshot.json"))
        obj = json.loads(files[target])
        obj["snapshot"]["narrative"] = "事后植入的叙述"
        files[target] = codec.canonical_json(obj)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        digest = next(c for c in rep["checks"] if c["code"] == "FILE_DIGEST")
        self.assertFalse(digest["ok"])
        self.assertTrue(any(target in d for d in digest["details"]))
        # 被篡改后绑定检查也不应通过/即使摘要通过绑定也会拦
        self.assertFalse(any(c["code"] == "SNAPSHOT_BINDING" and c["ok"]
                             for c in rep["checks"]))

    # -- 改清单文件哈希试图蒙混 -> 清单根/签名失败 --------------------------
    def test_rehash_tamper_breaks_root_and_signature(self):
        files = _unpack(self.good)
        target = next(n for n in files if n.startswith("export/snapshot.json"))
        obj = json.loads(files[target])
        obj["snapshot"]["narrative"] = "篡改"
        files[target] = codec.canonical_json(obj)
        manifest = json.loads(files["manifest.json"])
        for f in manifest["files"]:
            if f["path"] == target:
                f["sha256"] = codec.sha256_hex(files[target])
                f["size"] = len(files[target])
        files["manifest.json"] = codec.canonical_json(manifest)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        # 根失配且签名失配（签名覆盖 manifest 字节）
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "MANIFEST_ROOT")["ok"])
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "SIGNATURE")["ok"])

    # -- 直接改根字段 -------------------------------------------------------
    def test_manifest_root_field_tamper_detected(self):
        files = _unpack(self.good)
        manifest = json.loads(files["manifest.json"])
        manifest["root"] = "0" * 64
        files["manifest.json"] = codec.canonical_json(manifest)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "MANIFEST_ROOT")["ok"])
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "SIGNATURE")["ok"])

    # -- 签名被破坏 ---------------------------------------------------------
    def test_tampered_signature_detected(self):
        files = _unpack(self.good)
        sig = bytearray(files["manifest.sig"])
        sig[10] ^= 0xFF
        files["manifest.sig"] = bytes(sig)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "SIGNATURE")["ok"])

    # -- 审计链断裂（改时间线一帧）-----------------------------------------
    def test_broken_audit_chain_in_package_detected(self):
        files = _unpack(self.good)
        tl_name = f"timeline/{self.cid}.json"
        tl = json.loads(files[tl_name])
        # 改中间帧内容但保留 entry_hash -> 重算不符
        tl["events"][1]["summary"] = "被篡改"
        files[tl_name] = codec.canonical_json(tl)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        chain = next(c for c in rep["checks"] if c["code"] == "TIMELINE_CHAIN")
        self.assertFalse(chain["ok"])
        self.assertTrue(any("entry_hash 不匹配" in d for d in chain["details"]))

    def test_deleted_timeline_frame_detected(self):
        files = _unpack(self.good)
        tl_name = f"timeline/{self.cid}.json"
        tl = json.loads(files[tl_name])
        del tl["events"][1]
        files[tl_name] = codec.canonical_json(tl)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        chain = next(c for c in rep["checks"] if c["code"] == "TIMELINE_CHAIN")
        self.assertFalse(chain["ok"])

    # -- 多余文件 -----------------------------------------------------------
    def test_undeclared_extra_file_flagged(self):
        files = _unpack(self.good)
        files["evil.txt"] = b"not declared"
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "FILE_DIGEST")["ok"])

    # -- 不是合法包 ---------------------------------------------------------
    def test_garbage_input_fails_gracefully(self):
        rep = verify_package(b"this is not a tar at all")
        self.assertFalse(rep["valid"])
        self.assertFalse(next(c for c in rep["checks"]
                             if c["code"] == "PKG_OPEN")["ok"])
        self.assertIsNone(rep["package_root"])

    # -- 带外公钥指纹 -------------------------------------------------------
    def test_untrusted_signing_key_fingerprint_rejected(self):
        rep_ok = verify_package(self.good)
        real_fp = rep_ok["summary"]["public_key_fingerprint"]
        rep = verify_package(self.good, trusted_key_fingerprint="0" * 64)
        self.assertFalse(rep["valid"])
        self.assertTrue(any("指纹" in e for e in rep["errors"]))
        rep2 = verify_package(self.good, trusted_key_fingerprint=real_fp)
        self.assertTrue(rep2["valid"], rep2["errors"])

    # -- 多个失败项一次性全部列出 -------------------------------------------
    def test_multiple_failures_all_listed(self):
        files = _unpack(self.good)
        del files[next(n for n in files if n.startswith("sources/"))]
        files["evil.txt"] = b"x"
        sig = bytearray(files["manifest.sig"]); sig[0] ^= 1
        files["manifest.sig"] = bytes(sig)
        rep = verify_package(_repack(files))
        self.assertFalse(rep["valid"])
        failed_codes = {c["code"] for c in rep["checks"] if not c["ok"]}
        self.assertIn("FILE_PRESENT", failed_codes)
        self.assertIn("FILE_DIGEST", failed_codes)
        self.assertIn("SIGNATURE", failed_codes)


class TestOfflineCli(ExportTestBase):
    def test_cli_exit_codes_and_output(self):
        r = self.app.cases.intake("HOSPITAL", "H-CLI-1", self.zhang_patient())
        self.pump()
        t = self.exports.request_export(r["case_id"])
        self.exports.run_until_idle()
        path, _ = self.exports.package_path(t["task_id"])

        # 有效：退出码 0
        proc = subprocess.run(
            [sys.executable, "-m", "pv_service.verify_offline", str(path)],
            capture_output=True, text=True)
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertIn("有效", proc.stdout)

        # JSON 输出
        proc = subprocess.run(
            [sys.executable, "-m", "pv_service.verify_offline", str(path), "--json"],
            capture_output=True, text=True)
        self.assertEqual(0, proc.returncode)
        self.assertTrue(json.loads(proc.stdout)["valid"])

        # 篡改 -> 退出码 2
        files = _unpack(path.read_bytes())
        victim = next(n for n in files if n.startswith("sources/"))
        del files[victim]
        bad = path.with_name("bad.tar")
        bad.write_bytes(_repack(files))
        proc = subprocess.run(
            [sys.executable, "-m", "pv_service.verify_offline", str(bad)],
            capture_output=True, text=True)
        self.assertEqual(2, proc.returncode)
        self.assertIn("无效", proc.stdout)
        self.assertIn("FILE_PRESENT", proc.stdout)
        bad.unlink()

        # 不是包 -> 退出码 3
        junk = path.with_name("junk.tar")
        junk.write_bytes(b"garbage")
        proc = subprocess.run(
            [sys.executable, "-m", "pv_service.verify_offline", str(junk)],
            capture_output=True, text=True)
        self.assertEqual(3, proc.returncode)
        junk.unlink()
