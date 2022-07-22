import zlib, sys, hashlib

typ, path = sys.argv[1:]
h = zlib.adler32 if typ == "adler32" else zlib.crc32

with open(path, "rb") as f:
    c = h(b"")
    data=f.read(8*1024)
    while data:
        c = h(data, c)
        data = f.read(8*1024)
print("%x" % (c & 0xffffffff,))
