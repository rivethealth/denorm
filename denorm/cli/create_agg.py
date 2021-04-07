from ..agg import AggIo, create_agg
from .common import open_str_read, open_str_write


def cli(args):
    io = AggIo(
        config=lambda: open_str_read(args.schema),
        output=lambda: open_str_write(args.output),
    )
    create_agg(io)
