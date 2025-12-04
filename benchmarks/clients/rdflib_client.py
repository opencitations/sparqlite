from rdflib.plugins.stores.sparqlconnector import SPARQLConnector
from rdflib.query import Result

from clients.base import SPARQLClientBase


class RdflibClient(SPARQLClientBase):
    name = "rdflib"

    def setup(self, endpoint: str) -> None:
        self._connector_json = SPARQLConnector(
            query_endpoint=endpoint,
            update_endpoint=endpoint,
            method="POST",
            returnFormat="json",
        )
        self._connector_turtle = SPARQLConnector(
            query_endpoint=endpoint,
            update_endpoint=endpoint,
            method="POST",
            returnFormat="turtle",
        )

    def teardown(self) -> None:
        pass

    def select(self, query: str) -> Result:
        return self._connector_json.query(query)

    def ask(self, query: str) -> Result:
        return self._connector_json.query(query)

    def construct(self, query: str) -> Result:
        return self._connector_turtle.query(query)

    def describe(self, query: str) -> Result:
        return self._connector_turtle.query(query)

    def update(self, query: str) -> None:
        self._connector_json.update(query)
