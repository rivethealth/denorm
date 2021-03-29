import typing

T = typing.TypeVar("T")


class CycleError(Exception, typing.Generic[T]):
    def __init__(self, elements: typing.List[T]):
        self.elements = elements
        elements_str = " -> ".join(map(str, self.elements))
        super().__init__(f"Cycle: {elements_str}")


def recurse(start: T, fn: typing.Callable[[T], typing.Optional[T]]):
    visited: typing.Set[T] = set()
    chain: typing.List[T] = []

    while start is not None:
        if start in visited:
            raise CycleError(chain)

        yield start
        visited.add(start)
        chain.append(start)
        start = fn(start)
