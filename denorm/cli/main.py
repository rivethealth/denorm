import argparse

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command")

create_key_parser = subparsers.add_parser("create-agg")
create_key_parser.add_argument("--schema", default="-")
create_key_parser.add_argument("--output", default="-")

create_join_parser = subparsers.add_parser("create-join")
create_join_parser.add_argument("--schema", default="-")
create_join_parser.add_argument("--output", default="-")


def main():
    args = parser.parse_args()

    if args.command == "create-agg":
        from .create_agg import cli

        cli(args)
    if args.command == "create-join":
        from .create_join import cli

        cli(args)
