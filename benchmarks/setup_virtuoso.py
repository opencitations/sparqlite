from virtuoso_utilities.launch_virtuoso import check_container_exists, launch_virtuoso, remove_container

CONTAINER_NAME = "sparqlite-benchmark"
HTTP_PORT = 8091
ISQL_PORT = 1012
MEMORY = "4g"


def setup_virtuoso() -> None:
    launch_virtuoso(
        name=CONTAINER_NAME,
        http_port=HTTP_PORT,
        isql_port=ISQL_PORT,
        memory=MEMORY,
        enable_write_permissions=True,
        force_remove=True,
        wait_ready=True,
    )


def stop_virtuoso() -> None:
    if check_container_exists(CONTAINER_NAME):
        remove_container(CONTAINER_NAME)


def get_sparql_endpoint() -> str:
    return f"http://localhost:{HTTP_PORT}/sparql"
