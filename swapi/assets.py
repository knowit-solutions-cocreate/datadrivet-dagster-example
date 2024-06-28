
import dlt
from dlt.sources.helpers import requests
from dagster import asset, graph

# TODO: Verifiera att vi har data i snowflake
# X TODO: Ta bort staging assets
# TODO: Lägg till DLT



@asset()
def films() -> None:
    data = []
    url = 'https://swapi.dev/api/films/'
    response = requests.get(url)
    data.append(response.json()["results"])
    create_and_run_pipeline(data, url.split('/')[4].strip())

@asset()
def vehicles() -> None:
    data = []
    url = 'https://swapi.dev/api/vehicles/'
    response = requests.get(url)
    data.append(response.json()["results"])
    create_and_run_pipeline(data, url.split('/')[4].strip())

@asset()
def people() -> None:
    data = []
    url = 'https://swapi.dev/api/people/'
    response = requests.get(url)
    data.append(response.json()["results"])
    create_and_run_pipeline(data, url.split('/')[4].strip())

def create_and_run_pipeline(data, data_subject: str) -> None:

    pipeline = dlt.pipeline(
        pipeline_name=f"{data_subject}_pipeline",
        destination="snowflake",
        dataset_name=f"{data_subject}_data"
    )

    pipeline.run(data, table_name=f"{data_subject}")

@graph
def star_wars_graph():
    films()
    vehicles()
    people()

star_wars_job = star_wars_graph.to_job()


