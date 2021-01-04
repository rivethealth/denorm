def format(format, *args):
    i = 0
    state = "normal"
    replacement = ""
    result = ""
    while True:
        if state == "normal":
            if len(format) <= i:
                break
            elif format[i] == "$":
                state = "replacement"
                i += 1
            else:
                result += format[i]
                i += 1
        elif state == "replacement":
            if len(format) <= i:
                n = int(replacement) - 1
                if not (0 <= n < len(args)):
                    raise RuntimeError(f"Invalid param '{replacement}' in '{format}'")
                result += args[n]
                replacement = ""
                state = "normal"
            elif format[i] == "$":
                result += "$"
                state = "normal"
                i += 1
            elif format[i].isdigit():
                replacement += format[i]
                i += 1
            else:
                n = int(replacement) - 1
                if not (0 <= n < len(args)):
                    raise RuntimeError(f"Invalid param '{replacement}' in '{format}'")
                result += args[n]
                replacement = ""
                state = "normal"

    return result
