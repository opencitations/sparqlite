"""Integration tests for sparqlite against a real Virtuoso instance.

Uses OpenCitations data model for realistic test data.
All tests verify complete expected results, not just individual elements.
"""

import warnings

import pytest
from rdflib import RDF, Graph, Literal, Namespace, URIRef

from sparqlite import EndpointError, QueryError, SPARQLClient


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

FABIO = Namespace("http://purl.org/spar/fabio/")
DCTERMS = Namespace("http://purl.org/dc/terms/")
PRISM = Namespace("http://prismstandard.org/namespaces/basic/2.0/")
DATACITE = Namespace("http://purl.org/spar/datacite/")
LITERAL = Namespace("http://www.essepuntato.it/2010/06/literalreification/")
PRO = Namespace("http://purl.org/spar/pro/")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")

BR1 = URIRef("https://w3id.org/oc/meta/br/1")
BR2 = URIRef("https://w3id.org/oc/meta/br/2")
BR3 = URIRef("https://w3id.org/oc/meta/br/3")
RA1 = URIRef("https://w3id.org/oc/meta/ra/1")
RA2 = URIRef("https://w3id.org/oc/meta/ra/2")


class TestSelectQuery:
    """Tests for SELECT queries using OpenCitations data model."""

    def test_select_articles_with_titles(self, client, test_data):
        """Test SELECT query for journal articles with titles."""
        result = client.query(f"""
            {PREFIXES}
            SELECT ?article ?title
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?article a fabio:JournalArticle ;
                         dcterms:title ?title .
            }}
            ORDER BY ?title
        """)

        expected = [
            {
                "article": {"type": "uri", "value": "https://w3id.org/oc/meta/br/1"},
                "title": {"type": "literal", "value": "A study on citation networks"},
            },
            {
                "article": {"type": "uri", "value": "https://w3id.org/oc/meta/br/2"},
                "title": {"type": "literal", "value": "Machine learning in bibliometrics"},
            },
        ]

        assert isinstance(result, dict)
        assert result["results"]["bindings"] == expected

    def test_select_articles_with_dois(self, client, test_data):
        """Test SELECT query for articles with DOI identifiers."""
        result = client.query(f"""
            {PREFIXES}
            SELECT ?article ?title ?doi
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?article a fabio:JournalArticle ;
                         dcterms:title ?title ;
                         datacite:hasIdentifier ?id .
                ?id datacite:usesIdentifierScheme datacite:doi ;
                    literal:hasLiteralValue ?doi .
            }}
            ORDER BY ?doi
        """)

        expected = [
            {
                "article": {"type": "uri", "value": "https://w3id.org/oc/meta/br/1"},
                "title": {"type": "literal", "value": "A study on citation networks"},
                "doi": {"type": "literal", "value": "10.1000/test.001"},
            },
            {
                "article": {"type": "uri", "value": "https://w3id.org/oc/meta/br/2"},
                "title": {"type": "literal", "value": "Machine learning in bibliometrics"},
                "doi": {"type": "literal", "value": "10.1000/test.002"},
            },
        ]

        assert result["results"]["bindings"] == expected

    def test_select_authors_and_articles(self, client, test_data):
        """Test SELECT query for authors and their articles."""
        result = client.query(f"""
            {PREFIXES}
            SELECT ?name ?title
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?article a fabio:JournalArticle ;
                         dcterms:title ?title ;
                         pro:isDocumentContextFor ?role .
                ?role pro:withRole pro:author ;
                      pro:isHeldBy ?author .
                ?author foaf:name ?name .
            }}
            ORDER BY ?name
        """)

        expected = [
            {
                "name": {"type": "literal", "value": "Jane Doe"},
                "title": {"type": "literal", "value": "Machine learning in bibliometrics"},
            },
            {
                "name": {"type": "literal", "value": "John Smith"},
                "title": {"type": "literal", "value": "A study on citation networks"},
            },
        ]

        assert result["results"]["bindings"] == expected

    def test_select_alias(self, client, test_data):
        """Test select() method alias."""
        result = client.select(f"""
            {PREFIXES}
            SELECT ?article
            FROM <{TEST_GRAPH}>
            WHERE {{ ?article a fabio:JournalArticle }}
            ORDER BY ?article
        """)

        expected = [
            {"article": {"type": "uri", "value": "https://w3id.org/oc/meta/br/1"}},
            {"article": {"type": "uri", "value": "https://w3id.org/oc/meta/br/2"}},
        ]

        assert isinstance(result, dict)
        assert result["results"]["bindings"] == expected

    def test_empty_result(self, client, test_data):
        """Test empty result set."""
        result = client.query(f"""
            {PREFIXES}
            SELECT ?article
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?article dcterms:title "Nonexistent Title" .
            }}
        """)

        assert result["results"]["bindings"] == []


class TestAskQuery:
    """Tests for ASK queries using OpenCitations data model."""

    def test_ask_doi_exists(self, client, test_data):
        """Test ASK query to check if a DOI exists."""
        result = client.ask(f"""
            {PREFIXES}
            ASK {{
                GRAPH <{TEST_GRAPH}> {{
                    ?id datacite:usesIdentifierScheme datacite:doi ;
                        literal:hasLiteralValue "10.1000/test.001" .
                }}
            }}
        """)

        assert isinstance(result, bool)
        assert result is True

    def test_ask_doi_not_exists(self, client, test_data):
        """Test ASK query for non-existent DOI."""
        result = client.ask(f"""
            {PREFIXES}
            ASK {{
                GRAPH <{TEST_GRAPH}> {{
                    ?id literal:hasLiteralValue "10.9999/nonexistent" .
                }}
            }}
        """)

        assert isinstance(result, bool)
        assert result is False

    def test_ask_author_exists(self, client, test_data):
        """Test ASK query to check if an author exists."""
        result = client.ask(f"""
            {PREFIXES}
            ASK {{
                GRAPH <{TEST_GRAPH}> {{
                    ?author foaf:name "John Smith" .
                }}
            }}
        """)

        assert result is True

    def test_ask_book_exists(self, client, test_data):
        """Test ASK query to check if a book exists."""
        result = client.ask(f"""
            {PREFIXES}
            ASK {{
                GRAPH <{TEST_GRAPH}> {{
                    ?article a fabio:Book .
                }}
            }}
        """)

        assert isinstance(result, bool)
        assert result is True


class TestConstructQuery:
    """Tests for CONSTRUCT queries using OpenCitations data model."""

    def test_construct_author_article_relationships(self, client, test_data):
        """Test CONSTRUCT query for author-article relationships."""
        result = client.construct(f"""
            {PREFIXES}
            CONSTRUCT {{
                ?author foaf:name ?name ;
                        foaf:made ?article .
                ?article dcterms:title ?title .
            }}
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?article a fabio:JournalArticle ;
                         dcterms:title ?title ;
                         pro:isDocumentContextFor ?role .
                ?role pro:withRole pro:author ;
                      pro:isHeldBy ?author .
                ?author foaf:name ?name .
            }}
        """)

        expected = Graph()
        expected.add((RA1, FOAF.name, Literal("John Smith")))
        expected.add((RA1, FOAF.made, BR1))
        expected.add((BR1, DCTERMS.title, Literal("A study on citation networks")))
        expected.add((RA2, FOAF.name, Literal("Jane Doe")))
        expected.add((RA2, FOAF.made, BR2))
        expected.add((BR2, DCTERMS.title, Literal("Machine learning in bibliometrics")))

        assert isinstance(result, Graph)
        assert result.isomorphic(expected)

    def test_construct_article_metadata(self, client, test_data):
        """Test CONSTRUCT query for article metadata graph."""
        result = client.construct(f"""
            {PREFIXES}
            CONSTRUCT {{
                ?article a fabio:JournalArticle ;
                         dcterms:title ?title ;
                         prism:publicationDate ?date .
            }}
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?article a fabio:JournalArticle ;
                         dcterms:title ?title ;
                         prism:publicationDate ?date .
            }}
        """)

        expected = Graph()
        expected.add((BR1, RDF.type, FABIO.JournalArticle))
        expected.add((BR1, DCTERMS.title, Literal("A study on citation networks")))
        expected.add((BR1, PRISM.publicationDate, Literal("2024-01-15")))
        expected.add((BR2, RDF.type, FABIO.JournalArticle))
        expected.add((BR2, DCTERMS.title, Literal("Machine learning in bibliometrics")))
        expected.add((BR2, PRISM.publicationDate, Literal("2024-03-20")))

        assert isinstance(result, Graph)
        assert result.isomorphic(expected)

    def test_construct_book(self, client, test_data):
        """Test CONSTRUCT query for books."""
        result = client.construct(f"""
            {PREFIXES}
            CONSTRUCT {{ ?s ?p ?o }}
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?s a fabio:Book ;
                   ?p ?o .
            }}
        """)

        expected = Graph()
        expected.add((BR3, RDF.type, FABIO.Book))
        expected.add((BR3, DCTERMS.title, Literal("Introduction to Semantic Web")))
        expected.add((BR3, PRISM.publicationDate, Literal("2023-06-01")))

        assert isinstance(result, Graph)
        assert result.isomorphic(expected)

    def test_construct_empty(self, client, test_data):
        """Test CONSTRUCT with no results."""
        result = client.construct(f"""
            {PREFIXES}
            CONSTRUCT {{ ?s ?p ?o }}
            FROM <{TEST_GRAPH}>
            WHERE {{
                ?s a fabio:Thesis .
            }}
        """)

        expected = Graph()

        assert result.isomorphic(expected)


class TestDescribeQuery:
    """Tests for DESCRIBE queries using OpenCitations data model."""

    def test_describe_bibliographic_resource(self, client, test_data):
        """Test DESCRIBE query for a bibliographic resource."""
        result = client.describe(f"""
            {PREFIXES}
            DESCRIBE <https://w3id.org/oc/meta/br/1>
            FROM <{TEST_GRAPH}>
        """)

        expected = Graph()
        expected.add((BR1, RDF.type, FABIO.JournalArticle))
        expected.add((BR1, DCTERMS.title, Literal("A study on citation networks")))
        expected.add((BR1, PRISM.publicationDate, Literal("2024-01-15")))
        expected.add((BR1, DATACITE.hasIdentifier, URIRef("https://w3id.org/oc/meta/id/1")))
        expected.add((BR1, PRO.isDocumentContextFor, URIRef("https://w3id.org/oc/meta/ar/1")))

        assert isinstance(result, Graph)
        assert result.isomorphic(expected)


class TestUpdateQuery:
    """Tests for UPDATE queries using OpenCitations data model."""

    def test_update_insert_new_article(self, client):
        """Test INSERT DATA for a new article."""
        new_graph = "https://w3id.org/oc/meta/test/insert"
        client.update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH <{new_graph}> {{
                    <https://w3id.org/oc/meta/br/99> a fabio:JournalArticle ;
                        dcterms:title "Newly inserted article" ;
                        prism:publicationDate "2024-06-01" .
                }}
            }}
        """)

        result = client.ask(f"""
            {PREFIXES}
            ASK {{
                GRAPH <{new_graph}> {{
                    ?article dcterms:title "Newly inserted article" .
                }}
            }}
        """)
        assert bool(result) is True

        client.update(f"CLEAR GRAPH <{new_graph}>")

    def test_update_delete_article(self, client):
        """Test DELETE DATA for an article."""
        temp_graph = "https://w3id.org/oc/meta/test/delete"
        client.update(f"""
            {PREFIXES}
            INSERT DATA {{
                GRAPH <{temp_graph}> {{
                    <https://w3id.org/oc/meta/br/100> a fabio:JournalArticle ;
                        dcterms:title "Article to delete" .
                }}
            }}
        """)

        client.update(f"""
            {PREFIXES}
            DELETE DATA {{
                GRAPH <{temp_graph}> {{
                    <https://w3id.org/oc/meta/br/100> a fabio:JournalArticle ;
                        dcterms:title "Article to delete" .
                }}
            }}
        """)

        result = client.ask(f"""
            {PREFIXES}
            ASK {{
                GRAPH <{temp_graph}> {{
                    ?article dcterms:title "Article to delete" .
                }}
            }}
        """)
        assert result is False


class TestClientLifecycle:
    """Tests for client lifecycle management."""

    def test_context_manager(self, virtuoso):
        """Test context manager protocol."""
        with SPARQLClient(virtuoso) as client:
            result = client.ask("ASK { ?s ?p ?o }")
            assert isinstance(result, bool)

    def test_close_idempotent(self, virtuoso):
        """Test that close() can be called multiple times."""
        client = SPARQLClient(virtuoso)
        client.close()
        client.close()
        client.close()

    def test_connection_reuse(self, client, test_data):
        """Test that curl handle is reused across requests."""
        curl_id = id(client._curl)

        client.query(f"""
            {PREFIXES}
            SELECT * FROM <{TEST_GRAPH}> WHERE {{ ?s a fabio:JournalArticle }}
        """)
        assert id(client._curl) == curl_id

        client.ask(f"""
            {PREFIXES}
            ASK {{ GRAPH <{TEST_GRAPH}> {{ ?s a fabio:Book }} }}
        """)
        assert id(client._curl) == curl_id

    def test_resource_warning(self, virtuoso):
        """Test that ResourceWarning is raised on unclosed client."""
        client = SPARQLClient(virtuoso)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            del client

            assert len(w) == 1
            assert issubclass(w[0].category, ResourceWarning)
            assert "SPARQLClient was not closed" in str(w[0].message)


class TestErrorHandling:
    """Tests for error handling."""

    def test_query_error_syntax(self, client):
        """Test QueryError on syntax error."""
        with pytest.raises(QueryError):
            client.query("SELECT * WHERE { INVALID SYNTAX }")

    def test_endpoint_error_invalid_url(self):
        """Test EndpointError on invalid endpoint."""
        client = SPARQLClient("http://localhost:59999/nonexistent")

        with pytest.raises(EndpointError):
            client.ask("ASK { ?s ?p ?o }")

        client.close()


class TestRetryLogic:
    """Tests for retry logic."""

    def test_max_retries_reached(self):
        """Test that max retries are respected."""
        client = SPARQLClient(
            "http://localhost:59999/nonexistent",
            max_retries=0,
        )

        with pytest.raises(EndpointError):
            client.ask("ASK { ?s ?p ?o }")

        client.close()
