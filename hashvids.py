import struct
import itertools as it
import re


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


def find_col_statevid(records):
    head, snd = it.islice(records, 2)
    records = it.chain((head, snd), records)
    return next(i for i, cell in enumerate(snd)
                if re.match('NY[0-9]+', cell) is not None)


def find_col_checksum(records):
    head, snd = it.islice(records, 2)
    records = it.chain((head, snd), records)
    return next(i for i, cell in enumerate(snd)
                if re.match('[0-9a-f]{8}', cell))


if __name__ == '__main__':
    import csv
    from sys import stdin, stdout

    steno = csv.writer(stdout, dialect='unix')
    istrm = csv.reader(stdin)
    head = next(istrm)
    statevid = find_col_statevid(istrm)
    for record in istrm:
        try:
            record.append(hashvid(record[statevid]))
        except ValueError:
            continue
        steno.writerow(record)
