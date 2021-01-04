import re

_INDENT_RE = re.compile("^|(?<=\n)")


def indent(str: str, count: int) -> str:
    return re.sub(_INDENT_RE, "  " * count, str)
