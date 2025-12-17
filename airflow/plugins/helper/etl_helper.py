import hashlib

def encode_string_to_short_number(s: str, digits=16) -> str:
    h = hashlib.blake2b(s.encode(), digest_size=8)  # 8 bytes = 64-bit
    return str(int.from_bytes(h.digest(), 'big') % (10**digits))

def id128_hex(s: str) -> str:
    return hashlib.blake2b(
        s.encode("utf-8"),
        digest_size=16
    ).hexdigest()

