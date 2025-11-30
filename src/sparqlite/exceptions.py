"""Custom exceptions for sparqlite."""

from typing import Optional


class SPARQLError(Exception):
    """Base exception for all SPARQL-related errors."""


class QueryError(SPARQLError):
    """Raised when there is a syntax error in the SPARQL query."""


class EndpointError(SPARQLError):
    """Raised when there is an HTTP or connection error with the endpoint."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code
