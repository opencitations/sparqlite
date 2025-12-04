from abc import ABC, abstractmethod
from typing import Any, ClassVar


class SPARQLClientBase(ABC):
    name: ClassVar[str]

    @abstractmethod
    def setup(self, endpoint: str) -> None: ...

    @abstractmethod
    def teardown(self) -> None: ...

    @abstractmethod
    def select(self, query: str) -> Any: ...

    def ask(self, query: str) -> Any:
        raise NotImplementedError

    def construct(self, query: str) -> Any:
        raise NotImplementedError

    def describe(self, query: str) -> Any:
        raise NotImplementedError

    def update(self, query: str) -> Any:
        raise NotImplementedError
