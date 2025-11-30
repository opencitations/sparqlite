"""sparqlite - a modern, lightweight SPARQL 1.1 client for Python."""

from sparqlite.client import SPARQLClient
from sparqlite.exceptions import EndpointError, QueryError, SPARQLError

__version__ = "0.1.0"

__all__ = [
    "SPARQLClient",
    "SPARQLError",
    "QueryError",
    "EndpointError",
]
