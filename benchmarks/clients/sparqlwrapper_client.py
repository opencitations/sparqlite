from rdflib import Graph
from SPARQLWrapper import JSON, POST, RDFXML, SPARQLWrapper

from clients.base import SPARQLClientBase


class SPARQLWrapperClient(SPARQLClientBase):
    name = "sparqlwrapper"

    def setup(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._sparql = SPARQLWrapper(endpoint)

    def teardown(self) -> None:
        pass

    def select(self, query: str) -> dict:
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(JSON)
        return self._sparql.query().convert()

    def ask(self, query: str) -> dict:
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(JSON)
        return self._sparql.query().convert()

    def construct(self, query: str) -> Graph:
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(RDFXML)
        return self._sparql.query().convert()

    def describe(self, query: str) -> Graph:
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(RDFXML)
        return self._sparql.query().convert()

    def update(self, query: str) -> None:
        sparql = SPARQLWrapper(self._endpoint)
        sparql.setMethod(POST)
        sparql.setQuery(query)
        sparql.query()
