import re

_LINE_RE = re.compile("^|(?<=\n)")


def indent(str: str, count: int) -> str:
    return re.sub(_LINE_RE, "  " * count, str)
