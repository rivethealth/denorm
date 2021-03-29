from ..denorm import DenormIo, create_denorm
from .common import open_str_read, open_str_write


def cli(args):
    io = DenormIo(
        config=lambda: open_str_read(args.schema),
        output=lambda: open_str_write(args.output),
    )
    create_denorm(io)
