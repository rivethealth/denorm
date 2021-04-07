import argparse

from ..version import __version__


def main():
    parser = _create_parser()
    args = parser.parse_args()

    if args.command == "create-agg":
        from .create_agg import cli

        cli(args)
    if args.command == "create-join":
        from .create_join import cli

        cli(args)


def _create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="show version and exit",
    )

    subparsers = parser.add_subparsers(dest="command")

    _add_create_agg_command(subparsers)
    _add_create_join_command(subparsers)

    return parser


def _add_create_agg_command(subparsers):
    parser = subparsers.add_parser("create-agg")
    parser.add_argument("--schema", default="-")
    parser.add_argument("--output", default="-")


def _add_create_join_command(subparsers):
    parser = subparsers.add_parser("create-join")
    parser.add_argument("--schema", default="-")
    parser.add_argument("--output", default="-")
