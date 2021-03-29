from ..key import KeyIo, create_key
from .common import open_str_read, open_str_write


def cli(args):
    io = KeyIo(
        config=lambda: open_str_read(args.schema),
        output=lambda: open_str_write(args.output),
    )
    create_key(io)
