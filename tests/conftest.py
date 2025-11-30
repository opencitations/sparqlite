"""Pytest configuration and fixtures for sparqlite tests."""

import subprocess
import tempfile

import pytest
from virtuoso_utilities.launch_virtuoso import launch_virtuoso

from sparqlite import SPARQLClient


TEST_GRAPH = "https://w3id.org/oc/meta/test"

PREFIXES = """
PREFIX fabio: <http://purl.org/spar/fabio/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/>
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX pro: <http://purl.org/spar/pro/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
"""

OPENCITATIONS_TEST_DATA = f"""
{PREFIXES}

INSERT DATA {{
    GRAPH <{TEST_GRAPH}> {{
        <https://w3id.org/oc/meta/br/1> a fabio:JournalArticle ;
            dcterms:title "A study on citation networks" ;
            prism:publicationDate "2024-01-15" ;
            datacite:hasIdentifier <https://w3id.org/oc/meta/id/1> ;
            pro:isDocumentContextFor <https://w3id.org/oc/meta/ar/1> .

        <https://w3id.org/oc/meta/id/1>
            datacite:usesIdentifierScheme datacite:doi ;
            literal:hasLiteralValue "10.1000/test.001" .

        <https://w3id.org/oc/meta/ar/1>
            pro:withRole pro:author ;
            pro:isHeldBy <https://w3id.org/oc/meta/ra/1> .

        <https://w3id.org/oc/meta/ra/1> foaf:name "John Smith" .

        <https://w3id.org/oc/meta/br/2> a fabio:JournalArticle ;
            dcterms:title "Machine learning in bibliometrics" ;
            prism:publicationDate "2024-03-20" ;
            datacite:hasIdentifier <https://w3id.org/oc/meta/id/2> ;
            pro:isDocumentContextFor <https://w3id.org/oc/meta/ar/2> .

        <https://w3id.org/oc/meta/id/2>
            datacite:usesIdentifierScheme datacite:doi ;
            literal:hasLiteralValue "10.1000/test.002" .

        <https://w3id.org/oc/meta/ar/2>
            pro:withRole pro:author ;
            pro:isHeldBy <https://w3id.org/oc/meta/ra/2> .

        <https://w3id.org/oc/meta/ra/2> foaf:name "Jane Doe" .

        <https://w3id.org/oc/meta/br/3> a fabio:Book ;
            dcterms:title "Introduction to Semantic Web" ;
            prism:publicationDate "2023-06-01" .
    }}
}}
"""


@pytest.fixture(scope="session")
def virtuoso():
    """Launch Virtuoso container for test session."""
    container_name = "sparqlite-test-virtuoso"
    http_port = 8895

    with tempfile.TemporaryDirectory() as data_dir:
        launch_virtuoso(
            name=container_name,
            data_dir=data_dir,
            http_port=http_port,
            isql_port=11115,
            memory="2g",
            dba_password="dba",
            detach=True,
            wait_ready=True,
            enable_write_permissions=True,
            force_remove=True,
        )

        yield f"http://localhost:{http_port}/sparql"

        subprocess.run(
            ["docker", "rm", "-f", container_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


@pytest.fixture
def client(virtuoso):
    """Create SPARQLClient connected to test Virtuoso."""
    with SPARQLClient(virtuoso) as client:
        yield client


@pytest.fixture
def test_data(client):
    """Insert OpenCitations test data before each test and clean up after."""
    client.update(OPENCITATIONS_TEST_DATA)
    yield
    client.update(f"CLEAR GRAPH <{TEST_GRAPH}>")
