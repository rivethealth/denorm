import typing

T = typing.TypeVar("T")

ResourceFactory = typing.Callable[[], typing.ContextManager[T]]


class NoArgs:
    def __init__(self, fn):
        self._fn = fn
        self._resource = None

    def __enter__(self, *args, **kwargs):
        self._resource = self._fn()
        return self._resource.__enter__(*args, **kwargs)

    def __exit__(self, *args, **kwargs):
        self._resource.__exit__(*args, **kwargs)
        self._resource = None
