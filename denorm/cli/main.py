import argparse

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command")

create_parser = subparsers.add_parser("create-denorm")
create_parser.add_argument("--schema", default="-")
create_parser.add_argument("--output", default="-")


def main():
    args = parser.parse_args()

    if args.command == "create-denorm":
        from .create_denorm import cli

        cli(args)
