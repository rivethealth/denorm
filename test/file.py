import contextlib
import os
import tempfile


@contextlib.contextmanager
def temp_file(prefix=None):
    file, name = tempfile.mkstemp(prefix=prefix)
    os.close(file)
    try:
        yield name
    finally:
        os.remove(name)
