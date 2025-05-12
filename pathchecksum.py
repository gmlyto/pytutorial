import os
import hashlib
from fnmatch import fnmatch


def path_checksum(paths):
    ignore = [
        "*.pyc",
        "*.pyo",
        "*.log",
        "__pycache__",
    ]
    if not hasattr(paths, '__iter__'):
        raise TypeError('sequence or iterable expected not %r!' % type(paths))

    def _update_checksum(checksum, dirname, filenames):
        for filename in sorted(filenames):
            skip = False
            for p in ignore:
                if fnmatch(filename, p):
                    skip = True
                    break
            if skip:
                continue
            path = os.path.join(dirname, filename)
            checksum.update(filename)
            if os.path.isfile(path):
                fh = open(path, 'rb')
                while 1:
                    buf = fh.read(4096)
                    if not buf : break
                    checksum.update(buf)
                fh.close()

    chksum = hashlib.sha1()

    for path in sorted([os.path.normpath(f) for f in paths]):
        if os.path.exists(path):
            if os.path.isdir(path):
                os.path.walk(path, _update_checksum, chksum)
            elif os.path.isfile(path):
                _update_checksum(chksum, os.path.dirname(path), os.path.basename(path))

    return chksum.hexdigest()

    