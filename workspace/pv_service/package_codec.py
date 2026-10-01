"""证据包格式：确定性 tar + 内容清单 + Ed25519 签名。

包布局（tar 为无顶层目录的扁平命名，POSIX ustar 固定头）::

    package.json                包元数据（格式版本、导出时间、签名公钥指纹）
    manifest.json               文件清单 + 审计链头 + 各病例切片范围
    manifest.sig                对 manifest.json 原始字节的 Ed25519 签名（64B）
    export/request.json         发起导出时的请求（病例、版本、发起人、时间）
    export/snapshot.json        绑定的版本快照导出视图（来源/版本/关系/回执引用）
    cases/{caseId}.json         各病例的版本序列（只含截止版本）
    sources/{sourceId}.json     来源原文（source_records 行，payload 原样保留）
    reports/{reportId}.json     报送记录 + 截止时状态（由审计帧冻结）+ 回执
    relations.json              合并/拆分关系（截止锚点）
    timeline/{caseId}.json      各病例审计链切片（含 seq/prev_hash/entry_hash）

确定性（同一请求在任意进程/重启后字节一致 -> 内容寻址只留一份包）：
- JSON 一律 ``ensure_ascii=False, sort_keys=True, separators=(",",":")`` + "\\n"；
- tar 头 uid/gid/mtime 固定（mtime=0），文件按路径排序，无 pax/扩展头；
- 清单根 = sha256(canonical(manifest 除 root 外的稳定载荷))，签名覆盖
  manifest.json 字节，因此清单任何字段（含每个文件摘要）被改都验得出来。
"""
from __future__ import annotations

import hashlib
import io
import json
import tarfile
from typing import BinaryIO

from . import ed25519

FORMAT = "pv-evidence-package"
FORMAT_VERSION = 1

# tar 中清单与签名文件的保留名（核验时不参与“文件摘要”列表）
MANIFEST_NAME = "manifest.json"
SIGNATURE_NAME = "manifest.sig"
PACKAGE_META_NAME = "package.json"
RESERVED_NAMES = {MANIFEST_NAME, SIGNATURE_NAME, PACKAGE_META_NAME}


class PackageError(Exception):
    """包无法解析（不是合法的证据包）。"""


def canonical_json(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")) + "\n").encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# -- 清单根 ------------------------------------------------------------------

def manifest_root_payload(manifest: dict) -> dict:
    """参与清单根哈希的稳定载荷（排除 root 自身）。"""
    return {
        "format": manifest["format"],
        "format_version": manifest["format_version"],
        "package": manifest.get("package"),
        "request": manifest.get("request"),
        "snapshot": manifest.get("snapshot"),
        "case_id": manifest.get("case_id"),
        "version_id": manifest.get("version_id"),
        "files": manifest["files"],
        "timelines": manifest.get("timelines"),
        "relations": manifest.get("relations"),
        "anchor": manifest.get("anchor"),
    }


def compute_manifest_root(manifest: dict) -> str:
    payload = canonical_json(manifest_root_payload(manifest))
    return hashlib.sha256(payload).hexdigest()


# -- 写入 --------------------------------------------------------------------

def _tar_add(tar: tarfile.TarFile, name: str, data: bytes, mtime: int = 0) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    info.mtime = mtime
    info.mode = 0o444
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.type = tarfile.REGTYPE
    tar.addfile(info, io.BytesIO(data))


def build_package(out: BinaryIO, *, package_meta: dict, request: dict,
                  snapshot_view: dict, files: dict[str, dict],
                  timelines: list[dict], relations: list[dict],
                  anchor: dict, signer_seed: bytes) -> dict:
    """构建并写入证据包。

    参数:
        files: {path: {"media": "json"|"raw", "object": <可 JSON 序列化对象>
                       或 "data": bytes, "note": ...}}
        timelines: 清单用的审计切片摘要 [{case_id, head, first_seq,last_seq,
                     cut_rowid, n_events}]
    返回最终 manifest（含 root）。
    """
    # 1) 先序列化全部业务文件，按路径排序生成文件清单
    entries = {}
    for path in sorted(files):
        spec = files[path]
        if "data" in spec:
            data = spec["data"]
        else:
            data = canonical_json(spec["object"])
        entries[path] = data

    file_manifest = []
    for path in sorted(entries):
        file_manifest.append({
            "path": path,
            "sha256": sha256_hex(entries[path]),
            "size": len(entries[path]),
        })

    manifest = {
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "package": {
            "package_id": package_meta["package_id"],
            "created_at": package_meta["created_at"],
            "service": package_meta.get("service", "pv_service"),
        },
        "request": request,
        "snapshot": {
            "case_id": snapshot_view["case_id"],
            "version_id": snapshot_view["version_id"],
            "version_no": snapshot_view["version_no"],
            "change_type": snapshot_view["change_type"],
            "snapshot_sha256": snapshot_view["snapshot_sha256"],
        },
        "case_id": snapshot_view["case_id"],
        "version_id": snapshot_view["version_id"],
        "anchor": anchor,
        "files": file_manifest,
        "timelines": timelines,
        "relations": relations,
    }
    manifest["root"] = compute_manifest_root(manifest)
    manifest_bytes = canonical_json(manifest)

    public_key = ed25519.publickey(signer_seed)
    sig = ed25519.signature(manifest_bytes, signer_seed, public_key)
    meta = dict(package_meta)
    meta.setdefault("signing_key_algorithm", "Ed25519")
    meta["signing_public_key"] = public_key.hex()
    meta["signing_public_key_fingerprint"] = sha256_hex(public_key)
    meta["manifest_root"] = manifest["root"]

    # 2) 写确定性 tar：保留文件按名字排序，manifest/sig 固定在首尾位置
    with tarfile.open(fileobj=out, mode="w", format=tarfile.USTAR_FORMAT,
                      encoding="utf-8") as tar:
        _tar_add(tar, PACKAGE_META_NAME, canonical_json(meta))
        for path in sorted(entries):
            _tar_add(tar, path, entries[path])
        _tar_add(tar, MANIFEST_NAME, manifest_bytes)
        _tar_add(tar, SIGNATURE_NAME, sig)
    return manifest


# -- 读取 --------------------------------------------------------------------

def _is_within(base_parts: tuple[str, ...], name: str) -> bool:
    if name in ("", ".", "/"):
        return False
    parts = [p for p in name.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        return False
    return True


def read_package(source) -> dict:
    """读取证据包，返回 {manifest, signature, meta, files: {path: bytes}}。

    source 可以是文件路径、字节或可读二进制文件对象。
    任何结构问题抛 PackageError（对应核验 PKG_OPEN / PKG_MANIFEST 失败项）。
    """
    try:
        if isinstance(source, (bytes, bytearray)):
            fh = io.BytesIO(bytes(source))
            tar = tarfile.open(fileobj=fh, mode="r:*")
        elif isinstance(source, str):
            tar = tarfile.open(source, mode="r:*")
        else:
            tar = tarfile.open(fileobj=source, mode="r:*")
    except (tarfile.TarError, OSError) as e:
        raise PackageError(f"无法打开证据包（不是合法 tar）: {e}")

    files: dict[str, bytes] = {}
    manifest_bytes = None
    sig_bytes = None
    meta_bytes = None
    try:
        for member in tar:
            if not member.isfile():
                # 不允许目录/符号链接/设备等异常成员
                raise PackageError(f"非法成员类型 {member.type!r}: {member.name}")
            name = member.name.lstrip("./")
            if not _is_within((), name) or name.startswith("/"):
                raise PackageError(f"非法路径（越界/绝对路径）: {member.name}")
            f = tar.extractfile(member)
            data = f.read() if f else b""
            if name == MANIFEST_NAME:
                manifest_bytes = data
            elif name == SIGNATURE_NAME:
                sig_bytes = data
            elif name == PACKAGE_META_NAME:
                meta_bytes = data
            else:
                if name in files:
                    raise PackageError(f"包内重复文件: {name}")
                files[name] = data
    except tarfile.TarError as e:
        raise PackageError(f"tar 读取失败: {e}")
    finally:
        tar.close()

    if manifest_bytes is None:
        raise PackageError("缺少 manifest.json")
    if sig_bytes is None:
        raise PackageError("缺少 manifest.sig")
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise PackageError(f"manifest.json 不是合法 JSON: {e}")
    meta = None
    if meta_bytes is not None:
        try:
            meta = json.loads(meta_bytes.decode("utf-8"))
        except Exception:
            meta = None
    return {"manifest": manifest, "manifest_bytes": manifest_bytes,
            "signature": sig_bytes, "meta": meta, "files": files}
