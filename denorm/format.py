import enum
import typing


class _State(enum.Enum):
    TEXT = enum.auto()
    REPLACEMENT_START = enum.auto()
    REPLACEMENT = enum.auto()


def format(format: str, replacements: typing.Dict[str, str]):
    i = 0
    state = _State.TEXT
    replacement = ""
    result = ""
    while True:
        if state == _State.TEXT:
            if len(format) <= i:
                break
            elif format[i] == "$":
                state = _State.REPLACEMENT_START
                i += 1
            else:
                result += format[i]
                i += 1
        elif state == _State.REPLACEMENT_START:
            if len(format) <= i:
                raise RuntimeError(f"Expected {{ at end of input")
            elif format[i] == "$":
                result += "$"
                state = _State.TEXT
                i += 1
            elif format[i] != "{":
                raise RuntimeError(f"Expected {{ at character {i}")
            else:
                state = _State.REPLACEMENT
                i += 1
        elif state == _State.REPLACEMENT:
            if len(format) <= i:
                raise RuntimeError(f"Expected }} at end of input")
            elif format[i] == "}":
                try:
                    result += replacements[replacement]
                except KeyError:
                    raise RuntimeError(f"Invalid param '{replacement}'")
                replacement = ""
                state = _State.TEXT
                i += 1
            else:
                replacement += format[i]
                i += 1

    return result
