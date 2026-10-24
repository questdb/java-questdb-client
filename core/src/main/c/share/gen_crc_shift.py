# Generates the "append 256 zero bytes" operator for a raw (non-inverted) CRC-32C register,
# as four byte-indexed tables, and verifies it against a byte-wise reference CRC.
POLY = 0x82F63B78
def gf2_times(mat, vec):
    s = 0; i = 0
    while vec:
        if vec & 1: s ^= mat[i]
        vec >>= 1; i += 1
    return s
def square(mat): return [gf2_times(mat, mat[n]) for n in range(32)]
def zeros_op(nbytes):
    op = [POLY] + [1 << n for n in range(31)]  # multiply by x: one zero bit
    bits = 1
    while bits < 8 * nbytes: op = square(op); bits *= 2
    assert bits == 8 * nbytes
    return op
N = 256
op = zeros_op(N)
Z = [[gf2_times(op, n << (8 * k)) for n in range(256)] for k in range(4)]
def shift(c): return Z[0][c & 0xff] ^ Z[1][(c >> 8) & 0xff] ^ Z[2][(c >> 16) & 0xff] ^ Z[3][c >> 24]
def raw(c, data):
    for b in data:
        c ^= b
        for _ in range(8): c = (c >> 1) ^ (POLY if c & 1 else 0)
    return c
import os
for seed in (0xFFFFFFFF, 0, 0x12345678, raw(0xFFFFFFFF, os.urandom(37))):
    assert shift(seed) == raw(seed, bytes(N)), seed
# combine check: crc(A|B|C) == shift(shift(crc_A) ^ crc0_B) ^ crc0_C
A, B, C = (os.urandom(N) for _ in range(3))
assert raw(0xFFFFFFFF, A + B + C) == shift(shift(raw(0xFFFFFFFF, A)) ^ raw(0, B)) ^ raw(0, C)
rows = []
for k in range(4):
    rows.append("    {")
    for i in range(0, 256, 8):
        rows.append("        " + ", ".join("0x%08x" % v for v in Z[k][i:i + 8]) + ",")
    rows.append("    },")
print("\n".join(rows))
