# sparqlite

[![Tests](https://github.com/opencitations/sparqlite/actions/workflows/test.yml/badge.svg)](https://github.com/opencitations/sparqlite/actions/workflows/test.yml)
[![Coverage](https://byob.yarr.is/arcangelo7/badges/opencitations-sparqlite-coverage-main)](https://opencitations.github.io/sparqlite/coverage/)
[![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/opencitations/sparqlite)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-ISC-green)](https://github.com/opencitations/sparqlite/blob/master/LICENSE)

A modern, lightweight SPARQL 1.1 client for Python. Built on [pycurl](https://github.com/pycurl/pycurl) for [high performance](https://opencitations.github.io/sparqlite/architecture/why-pycurl/).

[Full documentation](https://opencitations.github.io/sparqlite/)

## Quick start

```bash
pip install sparqlite
```

```python
from sparqlite import SPARQLClient

with SPARQLClient("https://opencitations.net/meta/sparql") as client:
    result = client.query("""
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?article ?title WHERE {
            ?article a fabio:JournalArticle ;
                     dcterms:title ?title .
        }
        LIMIT 5
    """)

    for row in result["results"]["bindings"]:
        print(f"{row['title']['value']}")
```
