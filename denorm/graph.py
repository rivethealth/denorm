import orderedset


def recurse(start, fn) -> list:
    result = orderedset.OrderedSet()

    while start is not None:
        if start in result:
            raise RuntimeError(f"Cycle: {' -> '.join(map(str, result))}")

        result.add(start)
        start = fn(start)

    return list(result)
