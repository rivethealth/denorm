from ..join import JoinIo, create_join
from .common import open_str_read, open_str_write


def cli(args):
    io = JoinIo(
        config=lambda: open_str_read(args.schema),
        output=lambda: open_str_write(args.output),
    )
    create_join(io)
