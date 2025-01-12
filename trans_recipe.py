import struct
import sys

if len(sys.argv) != 3:
    print("Usage: python metaPath dstPath")
    sys.exit(1)

metaPath = sys.argv[1]
dstPath = sys.argv[2]

with open(metaPath, 'rb') as f:
    data = f.read()

i = 0
def read(size):
    global i
    res = data[i:i + size]
    i += size
    return res

lst = []
rubbish = read(28)
pathLen, = struct.unpack('<i', rubbish[-4:])
path = read(pathLen)
print(path)

offset = 0
while True:
    nameLen = read(4)
    if not nameLen:
        break
    num, = struct.unpack('<i', nameLen)

    filename = read(num)
    print(filename)

    chunknum = read(8)
    fileSize = read(8)
    lst.append((nameLen, filename, chunknum, fileSize, offset))
    length, = struct.unpack('<q', chunknum)
    offset += length

print(offset)

lst.sort(key=lambda x: hash(x[1]))

with open(dstPath, "wb") as f:
    for recipe in lst:
        assert len(recipe[2]) == 8
        f.write(recipe[2])
        b = struct.pack('<q', recipe[4])
        assert len(b) == 8
        f.write(b)

