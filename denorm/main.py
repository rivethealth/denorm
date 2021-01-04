import argparse

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command")

create_parser = subparsers.add_parser("create_sql")
create_parser.add_argument("input")


def main():
    args = parser.parse_args()

    if args.command == "create_sql":
        from .create_sql import cli

        cli(args)
