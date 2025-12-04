from sparqlite import SPARQLClient

from clients.base import SPARQLClientBase


class SparqliteClient(SPARQLClientBase):
    name = "sparqlite"

    def setup(self, endpoint: str) -> None:
        self._client = SPARQLClient(endpoint, max_retries=0)

    def teardown(self) -> None:
        self._client.close()

    def select(self, query: str) -> dict:
        return self._client.query(query)

    def ask(self, query: str) -> bool:
        return self._client.ask(query)

    def construct(self, query: str) -> bytes:
        return self._client.construct(query)

    def describe(self, query: str) -> bytes:
        return self._client.describe(query)

    def update(self, query: str) -> None:
        self._client.update(query)
