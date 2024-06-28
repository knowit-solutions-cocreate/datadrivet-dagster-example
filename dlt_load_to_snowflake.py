import dlt
from dlt.sources.helpers import requests


def load_film():

    url = "https://swapi.dev/api/films/"

    data = []
    response = requests.get(url)
    response.raise_for_status()
    data.append(response.json())

    pipeline = dlt.pipeline(
        pipeline_name="film",
        destination="snowflake",
        dataset_name="film_data",
    )

    pipeline.run(data, table_name="movies")


if __name__ == "__main__":
    # Add credentials in the .dlt/secrets.toml file 
    load_film()
