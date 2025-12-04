from clients.base import SPARQLClientBase
from clients.rdflib_client import RdflibClient
from clients.sparqlite_client import SparqliteClient
from clients.sparqlwrapper_client import SPARQLWrapperClient

CLIENTS: list[type[SPARQLClientBase]] = [SparqliteClient, SPARQLWrapperClient, RdflibClient]
