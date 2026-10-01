"""离线核验命令行：在不连接服务、不访问数据库的另一进程中核验证据包。

用法::

    python -m pv_service.verify_offline PACKAGE.tar [--fingerprint HEX]
        [--json] [--print-key]

退出码：0=有效；2=核验失败（逐项打印失败项）；3=包无法打开；1=用法错误。
公钥指纹可通过带外渠道（服务启动日志 / 运维发布的指纹）用 --fingerprint
固定，防止包内自携公钥被替换的攻击。
"""
from __future__ import annotations

import argparse
import json
import sys

from . import package_codec as codec
from .verifier import verify_package


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="药物警戒证据包离线核验")
    ap.add_argument("package", help="证据包 .tar 路径")
    ap.add_argument("--fingerprint", help="带外信任的签名公钥 SHA-256 指纹（hex）")
    ap.add_argument("--json", action="store_true", help="以 JSON 输出完整报告")
    ap.add_argument("--print-key", action="store_true",
                    help="仅打印包内签名公钥指纹后退出（供带外比对）")
    args = ap.parse_args(argv)

    if args.print_key:
        try:
            pkg = codec.read_package(args.package)
        except codec.PackageError as e:
            print(f"无法打开包: {e}", file=sys.stderr)
            return 3
        meta = pkg.get("meta") or {}
        print(meta.get("signing_public_key_fingerprint", ""))
        return 0

    report = verify_package(args.package,
                            trusted_key_fingerprint=args.fingerprint)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        root = report.get("package_root")
        print(f"证据包: {args.package}")
        print(f"清单根: {root}")
        s = report.get("summary") or {}
        print(f"病例: {s.get('case_id')}  版本: {s.get('version_id')}  "
              f"文件: {s.get('files')}  审计切片: {s.get('timelines')}  "
              f"关系: {s.get('relations')}  报告: {s.get('reports')}")
        print(f"公钥指纹: {s.get('public_key_fingerprint')}")
        print("-" * 68)
        for c in report["checks"]:
            mark = "PASS" if c["ok"] else "FAIL"
            print(f"[{mark}] {c['code']:<16} {c['message']}")
        if report["warnings"]:
            print("-" * 68)
            for w in report["warnings"]:
                print(f"[WARN] {w}")
        if report["errors"]:
            print("-" * 68)
            print("失败项明细：")
            for e in report["errors"]:
                print(f"  - {e}")
        print("-" * 68)
        print("结论:", "有效 ✓" if report["valid"] else "无效 ✗（不得采信该包）")

    if report["valid"]:
        return 0
    return 2 if report.get("package_root") else 3


if __name__ == "__main__":
    sys.exit(main())
