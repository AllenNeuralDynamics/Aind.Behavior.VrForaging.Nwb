import abc
import typing as ty


class AbstractProcessor(abc.ABC):
    _raise_on_error: bool = False

    @abc.abstractmethod
    def process(self):
        raise NotImplementedError("Subclasses must implement the process method.")

    def with_raise_errors(self, raise_on_error: bool = True) -> ty.Self:
        self._raise_on_error = raise_on_error
        return self

    @property
    def raise_on_error(self) -> bool:
        return self._raise_on_error
