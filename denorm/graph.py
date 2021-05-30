import typing

T = typing.TypeVar("T")


class CycleError(Exception, typing.Generic[T]):
    def __init__(self, elements: typing.List[T]):
        self.elements = elements
        elements_str = " -> ".join(map(str, self.elements))
        super().__init__(f"Cycle: {elements_str}")


def closure(start: typing.List[T], fn: typing.Callable[[T], typing.List[T]]):
    visited: typing.Set[T] = set()
    chain: typing.List[T] = []

    def visit(item: T):
        if item in chain:
            raise CycleError(chain[chain.index(item) :])

        if item in visited:
            return

        chain.append(item)
        visited.add(item)
        for dep in fn(item):
            yield from visit(dep)
        chain.pop()

        yield item

    for item in start:
        yield from visit(item)
