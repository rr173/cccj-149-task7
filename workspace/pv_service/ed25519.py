"""纯 Python Ed25519（RFC 8032），零第三方依赖。

证据包需要在“另一进程 / 另一台机器”上离线核验签名，而环境中没有
cryptography 等第三方库；这里给出 RFC 8032 参考实现的直接移植
（ref10 风格的完整基点标量乘，常量时间性不是本模块目标——核验的是
*完整性与来源*，而非抵御本机侧信道）。

格式：
- 私钥种子 32 字节；公钥 32 字节；签名 64 字节（R||S 各 32）；
- 存储/传输统一用 hex（manifest 内）或裸字节（tar 文件）；
- tests/test_ed25519.py 用 RFC 8032 官方测试向量验证。
"""
from __future__ import annotations

import hashlib
import os
import base64

q = 2 ** 255 - 19
l = 2 ** 252 + 27742317777372353535851937790883648493


def _H(m: bytes) -> bytes:
    return hashlib.sha512(m).digest()


def _expmod(b: int, e: int, m: int) -> int:
    return pow(b, e, m)


def _inv(x: int) -> int:
    return _expmod(x, q - 2, q)


d = -121665 * _inv(121666) % q
I = _expmod(2, (q - 1) // 4, q)


def _xrecover(y: int) -> int:
    xx = (y * y - 1) * _inv(d * y * y + 1)
    x = _expmod(xx, (q + 3) // 8, q)
    if (x * x - xx) % q != 0:
        x = (x * I) % q
    if x % 2 != 0:
        x = q - x
    return x


By = 4 * _inv(5)
Bx = _xrecover(By)
B = (Bx, By)


def _edwards(P, Q):
    x1, y1 = P
    x2, y2 = Q
    x3 = (x1 * y2 + x2 * y1) * _inv(1 + d * x1 * x2 * y1 * y2)
    y3 = (y1 * y2 + x1 * x2) * _inv(1 - d * x1 * x2 * y1 * y2)
    return x3 % q, y3 % q


def _scalarmult(P, e: int):
    if e == 0:
        return (0, 1)
    Q = _scalarmult(P, e // 2)
    Q = _edwards(Q, Q)
    if e & 1:
        Q = _edwards(Q, P)
    return Q


def _encodeint(y: int) -> bytes:
    return y.to_bytes(32, "little")


def _decodeint(s: bytes) -> int:
    return int.from_bytes(s, "little")


def _encodepoint(P) -> bytes:
    x, y = P
    # RFC 8032：255 位 y + 最高位放 x 的符号
    v = y | ((x & 1) << 255)
    return v.to_bytes(32, "little")


def _bit(h: bytes, i: int) -> int:
    """djb/SUPERCOP 约定的位序：字节内低序位为 bit 0。"""
    return (h[i // 8] >> (i % 8)) & 1


def _decodepoint(s: bytes):
    y = int.from_bytes(s, "little") & ((1 << 255) - 1)
    x = _xrecover(y)
    if (x & 1) != _bit(s, 255):
        x = q - x
    P = (x, y)
    if not _isoncurve(P):
        raise ValueError("解压点不在曲线上（公钥/签名非法）")
    return P


def _isoncurve(P) -> bool:
    x, y = P
    return (-x * x + y * y - 1 - d * x * x * y * y) % q == 0


def _scalar_uniform(h: bytes) -> int:
    """RFC 8032：clamp 后标量（清低 3 位；第 254 位置 1、第 255 位清 0）。"""
    a = 2 ** 254 + sum(2 ** i * _bit(h, i) for i in range(3, 254))
    return a


def publickey(seed: bytes) -> bytes:
    if len(seed) != 32:
        raise ValueError("私钥种子必须为 32 字节")
    h = _H(seed)
    a = _scalar_uniform(h)
    A = _scalarmult(B, a)
    return _encodepoint(A)


def _hint(m: bytes) -> int:
    h = _H(m)
    return int.from_bytes(h, "little") % l


def signature(m: bytes, seed: bytes, pk: bytes | None = None) -> bytes:
    h = _H(seed)
    a = _scalar_uniform(h)
    A = pk or publickey(seed)
    r = _hint(h[32:64] + m)
    R = _scalarmult(B, r)
    Rb = _encodepoint(R)
    S = (r + _hint(Rb + A + m) * a) % l
    return Rb + _encodeint(S)


def verify(public: bytes, m: bytes, sig: bytes) -> bool:
    """签名合法返回 True；任何格式/曲线/等式错误返回 False（不抛异常）。"""
    try:
        if len(public) != 32 or len(sig) != 64:
            return False
        R = _decodepoint(sig[:32])
        A = _decodepoint(public)
        S = _decodeint(sig[32:])
        if S >= l:
            return False
        h = _hint(sig[:32] + public + m)
        return _scalarmult(B, S) == _edwards(R, _scalarmult(A, h))
    except Exception:
        return False


# -- 便捷封装：密钥对文件 + base64 ------------------------------------------

def generate_keypair() -> tuple[bytes, bytes]:
    seed = os.urandom(32)
    return seed, publickey(seed)


def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))
