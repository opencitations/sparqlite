import httpx

from setup_virtuoso import get_sparql_endpoint

BASE = "http://example.org/"
GRAPH = "http://example.org/benchmark"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"


def _generate_triples(num_entities: int, start: int = 0) -> list[str]:
    triples = []
    for i in range(start, start + num_entities):
        subject = f"<{BASE}entity/{i}>"
        triples.append(f"{subject} <{RDF_TYPE}> <{BASE}Entity> .")
        triples.append(f'{subject} <{RDFS_LABEL}> "Entity {i}" .')
        triples.append(f'{subject} <{BASE}value> "{i}"^^<{XSD_INTEGER}> .')
        triples.append(f"{subject} <{BASE}category> <{BASE}category/{i % 10}> .")
        if i > 0:
            triples.append(f"{subject} <{BASE}relatedTo> <{BASE}entity/{i - 1}> .")
    return triples


def generate_insert_sparql(num_entities: int = 1000, start: int = 0) -> str:
    triples = " ".join(_generate_triples(num_entities, start))
    return f"INSERT DATA {{ GRAPH <{GRAPH}> {{ {triples} }} }}"


def load_test_data(total_entities: int = 1000, batch_size: int = 100) -> None:
    endpoint = get_sparql_endpoint()
    with httpx.Client(timeout=60.0) as client:
        for start in range(0, total_entities, batch_size):
            insert_query = generate_insert_sparql(batch_size, start)
            response = client.post(endpoint, content=insert_query, headers={"Content-Type": "application/sparql-update"})
            response.raise_for_status()