"""Synchronous SPARQL client."""

import json
import time
import warnings
from io import BytesIO
from urllib.parse import urlencode

import pycurl

from sparqlite.exceptions import EndpointError, QueryError


class SPARQLClient:
    """Synchronous SPARQL 1.1 client with connection pooling and automatic retry."""

    def __init__(
        self,
        endpoint: str,
        *,
        max_retries: int = 5,
        backoff_factor: float = 0.5,
        timeout: float | None = None,
    ):
        """Initialize the SPARQL client.

        Args:
            endpoint: The SPARQL endpoint URL.
            max_retries: Maximum number of retry attempts for transient errors.
            backoff_factor: Factor for exponential backoff (wait = factor * 2^retry).
            timeout: Request timeout in seconds. None means no timeout.
        """
        self.endpoint = endpoint
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self._curl = pycurl.Curl()

    def __enter__(self) -> "SPARQLClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        if self._curl is not None:
            warnings.warn(
                "SPARQLClient was not closed. Use 'with SPARQLClient(...) as client:' "
                "or call 'client.close()' explicitly.",
                ResourceWarning,
                stacklevel=2,
            )
        self.close()

    def close(self) -> None:
        """Close the client and release resources.

        This method is idempotent - calling it multiple times is safe.
        """
        if self._curl is not None:
            self._curl.close()
            self._curl = None

    def _request(
        self,
        query: str,
        accept: str,
        *,
        is_update: bool = False,
    ) -> bytes:
        """Execute an HTTP request with retry logic."""
        last_error = None

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                wait_time = self.backoff_factor * (2**attempt)
                time.sleep(wait_time)

            buffer = BytesIO()
            self._curl.reset()

            if self.timeout is not None:
                self._curl.setopt(pycurl.TIMEOUT_MS, int(self.timeout * 1000))

            try:
                self._curl.setopt(pycurl.URL, self.endpoint)
                self._curl.setopt(pycurl.WRITEDATA, buffer)
                self._curl.setopt(
                    pycurl.HTTPHEADER,
                    [
                        f"Accept: {accept}",
                        "User-Agent: sparqlite/0.1.0",
                    ],
                )

                if is_update:
                    post_data = urlencode({"update": query})
                else:
                    post_data = urlencode({"query": query})

                self._curl.setopt(pycurl.POSTFIELDS, post_data)
                self._curl.perform()

                status_code = self._curl.getinfo(pycurl.RESPONSE_CODE)

                if status_code == 400:
                    raise QueryError(f"Query syntax error: {buffer.getvalue().decode()}")

                if status_code >= 500:
                    last_error = EndpointError(
                        f"Server error: {status_code}",
                        status_code=status_code,
                    )
                    continue

                if status_code >= 400:
                    raise EndpointError(
                        f"HTTP error: {status_code} - {buffer.getvalue().decode()}",
                        status_code=status_code,
                    )

                return buffer.getvalue()

            except pycurl.error as e:
                error_code, error_msg = e.args
                if error_code in (pycurl.E_COULDNT_CONNECT, pycurl.E_COULDNT_RESOLVE_HOST):
                    last_error = EndpointError(f"Connection error: {error_msg}")
                elif error_code == pycurl.E_OPERATION_TIMEDOUT:
                    last_error = EndpointError(f"Timeout error: {error_msg}")
                else:
                    last_error = EndpointError(f"Request error: {error_msg}")
                continue

        raise last_error

    def query(self, query: str) -> dict:
        """Execute a SELECT query.

        Args:
            query: The SPARQL SELECT query string.

        Returns:
            Dictionary with SPARQL JSON results format.
        """
        content = self._request(query, "application/sparql-results+json")
        return json.loads(content)

    def select(self, query: str) -> dict:
        """Execute a SELECT query. Alias for query()."""
        return self.query(query)

    def ask(self, query: str) -> bool:
        """Execute an ASK query.

        Args:
            query: The SPARQL ASK query string.

        Returns:
            Boolean result of the ASK query.
        """
        content = self._request(query, "application/sparql-results+json")
        return json.loads(content)["boolean"]

    def construct(self, query: str) -> bytes:
        """Execute a CONSTRUCT query.

        Args:
            query: The SPARQL CONSTRUCT query string.

        Returns:
            Raw N-Triples bytes.
        """
        return self._request(query, "application/n-triples")

    def describe(self, query: str) -> bytes:
        """Execute a DESCRIBE query.

        Args:
            query: The SPARQL DESCRIBE query string.

        Returns:
            Raw N-Triples bytes.
        """
        return self._request(query, "application/n-triples")

    def update(self, query: str) -> None:
        """Execute a SPARQL UPDATE query (INSERT, DELETE, etc.).

        Args:
            query: The SPARQL UPDATE query string.
        """
        self._request(query, "application/sparql-results+json", is_update=True)
