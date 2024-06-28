
import dlt
from dlt.sources.helpers import requests
from dagster import asset, graph
import pandas as pd

# TODO: Verifiera att vi har data i snowflake
# X TODO: Ta bort staging assets
# TODO: Lägg till DLT



@asset()
def films() -> None:
    data = []
    response = requests.get('https://swapi.dev/api/films/')
    data.append(response.json()["results"])
    create_and_run_pipeline(data)

@asset(
    key_prefix=["raw_swapi"]
)
def vehicles() -> pd.DataFrame:
    response = requests.get('https://swapi.dev/api/vehicles/')
    return pd.DataFrame(response.json()["results"])

@asset(
    key_prefix=["raw_swapi"]
)
def people() -> pd.DataFrame:
    response = requests.get('https://swapi.dev/api/people/')
    return pd.DataFrame(response.json()["results"])

def create_and_run_pipeline(data) -> None:

    pipeline = dlt.pipeline(
        pipeline_name="film",
        destination="snowflake",
        dataset_name="film_data3"
    )

    pipeline.run(data, table_name="film")

@graph
def star_wars_graph():
    films()
    vehicles()
    people()

star_wars_job = star_wars_graph.to_job()


