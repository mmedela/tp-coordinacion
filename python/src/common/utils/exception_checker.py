class ExceptionChecker:

    def __init__(self, *mappings: tuple[type[Exception] | tuple[type[Exception], ...], type[Exception]]):
        self._mappings = mappings

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return False

        for source_types, target_type in self._mappings:
            if issubclass(exc_type, source_types):
                raise target_type(str(exc_value)) from exc_value

        return False