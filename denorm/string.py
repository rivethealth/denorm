import re

_LINE_RE = re.compile("^|(?<=\n)")


def indent(str: str, count: int) -> str:
    return "\n".join("  " * count + line for line in str.split("\n"))
