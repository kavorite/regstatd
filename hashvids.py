import struct


def hashvid(vid):
    # fnv—1a on the bits of the little-endian-packed voter ID int
    fnv_offset_basis = 0x811c9dc5
    fnv_prime = 0x01000193
    fnv = fnv_offset_basis
    for b in struct.pack('<I', int(vid[2:]) & 0xffffffff):
        fnv ^= b
        fnv *= fnv_prime
        fnv &= 0xffffffff
    return struct.pack('<I', fnv).hex()


if __name__ == '__main__':
    import csv
    from sys import stdin, stdout
    steno = csv.writer(stdout, dialect='unix')
    for record in csv.reader(stdin):
        record.append(hashvid(record[38]))
        steno.writerow(record)
