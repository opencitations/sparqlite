"""Unit tests for error handling edge cases using mocks."""

from unittest.mock import MagicMock, patch

import pycurl
import pytest

from sparqlite import EndpointError, SPARQLClient


class TestHTTPErrorHandling:
    """Tests for HTTP error handling."""

    def test_server_error_500_triggers_retry(self):
        """Test that 500 errors trigger retry logic."""
        mock_curl = MagicMock()
        mock_curl.getinfo.return_value = 500

        call_count = 0

        def mock_perform():
            nonlocal call_count
            call_count += 1

        mock_curl.perform = mock_perform

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("http://example.org/sparql", max_retries=2)
            client._curl = mock_curl

            with pytest.raises(EndpointError) as exc_info:
                client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")

            assert "500" in str(exc_info.value)
            assert exc_info.value.status_code == 500
            assert call_count == 3

            client.close()

    def test_http_403_forbidden(self):
        """Test that 403 errors raise EndpointError without retry."""
        mock_curl = MagicMock()
        mock_curl.getinfo.return_value = 403

        buffer_content = b"Forbidden"

        def mock_setopt(opt, val):
            if opt == pycurl.WRITEDATA:
                val.write(buffer_content)

        mock_curl.setopt = mock_setopt

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("http://example.org/sparql")
            client._curl = mock_curl

            with pytest.raises(EndpointError) as exc_info:
                client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")

            assert exc_info.value.status_code == 403
            assert "403" in str(exc_info.value)

            client.close()


class TestPycurlErrorHandling:
    """Tests for pycurl error handling."""

    def test_timeout_error(self):
        """Test that timeout errors are handled."""
        mock_curl = MagicMock()
        mock_curl.perform.side_effect = pycurl.error(
            pycurl.E_OPERATION_TIMEDOUT, "Operation timed out"
        )

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("http://example.org/sparql", max_retries=0)
            client._curl = mock_curl

            with pytest.raises(EndpointError) as exc_info:
                client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")

            assert "Timeout" in str(exc_info.value)

            client.close()

    def test_resolve_host_error(self):
        """Test that host resolution errors are handled."""
        mock_curl = MagicMock()
        mock_curl.perform.side_effect = pycurl.error(
            pycurl.E_COULDNT_RESOLVE_HOST, "Could not resolve host"
        )

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("http://nonexistent.invalid/sparql", max_retries=0)
            client._curl = mock_curl

            with pytest.raises(EndpointError) as exc_info:
                client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")

            assert "Connection error" in str(exc_info.value)

            client.close()

    def test_generic_pycurl_error(self):
        """Test that generic pycurl errors are handled."""
        mock_curl = MagicMock()
        mock_curl.perform.side_effect = pycurl.error(
            pycurl.E_SSL_CONNECT_ERROR, "SSL connection error"
        )

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("https://example.org/sparql", max_retries=0)
            client._curl = mock_curl

            with pytest.raises(EndpointError) as exc_info:
                client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")

            assert "Request error" in str(exc_info.value)

            client.close()


class TestTimeoutConfiguration:
    """Tests for timeout configuration."""

    def test_timeout_parameter_stored(self):
        """Test that timeout parameter is stored correctly."""
        with patch("pycurl.Curl") as mock_curl_class:
            mock_curl = MagicMock()
            mock_curl_class.return_value = mock_curl

            client = SPARQLClient("http://example.org/sparql", timeout=30.0)
            assert client.timeout == 30.0
            client.close()

    def test_timeout_default_is_none(self):
        """Test that timeout defaults to None."""
        with patch("pycurl.Curl") as mock_curl_class:
            mock_curl = MagicMock()
            mock_curl_class.return_value = mock_curl

            client = SPARQLClient("http://example.org/sparql")
            assert client.timeout is None
            client.close()

    def test_timeout_ms_set_when_timeout_provided(self):
        """Test that TIMEOUT_MS is set when timeout is provided."""
        mock_curl = MagicMock()
        mock_curl.getinfo.return_value = 200

        setopt_calls = []

        def track_setopt(opt, val):
            setopt_calls.append((opt, val))

        mock_curl.setopt = track_setopt

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("http://example.org/sparql", timeout=5.5)
            client._curl = mock_curl
            client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")
            client.close()

        timeout_calls = [(opt, val) for opt, val in setopt_calls if opt == pycurl.TIMEOUT_MS]
        assert timeout_calls == [(pycurl.TIMEOUT_MS, 5500)]

    def test_timeout_ms_not_set_when_timeout_none(self):
        """Test that TIMEOUT_MS is not set when timeout is None."""
        mock_curl = MagicMock()
        mock_curl.getinfo.return_value = 200

        setopt_calls = []

        def track_setopt(opt, val):
            setopt_calls.append((opt, val))

        mock_curl.setopt = track_setopt

        with patch("pycurl.Curl", return_value=mock_curl):
            client = SPARQLClient("http://example.org/sparql")
            client._curl = mock_curl
            client._request("SELECT * WHERE { ?s ?p ?o }", "application/json")
            client.close()

        timeout_calls = [opt for opt, val in setopt_calls if opt == pycurl.TIMEOUT_MS]
        assert timeout_calls == []
