import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, dag
from airflow.models import Variable

'Getting the URL from Airflow Variable'
url = Variable.get("url")

@dag(
    dag_id="stadia_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"owner": "Astro", "retries": 1},
    tags=["example"],
)
def stadia_pipeline_dag():
    #url = "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"

    @task
    def get_data(url):
        # Fetch the HTML content
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML using BeautifulSoup
        soup = bs(response.text, "html.parser")

        # Find the specific table by class
        table = soup.find("table", {"class": "wikitable sortable sticky-header"})

        # Extract table headers
        headers = [th.text.strip() for th in table.find_all("th")]

        # Extract table rows
        rows = []
        base_url = "https://en.wikipedia.org"  # Base URL for relative image links

        for tr in table.find_all("tr")[1:]:  # Skip the header row
            cells = []
            for td in tr.find_all(["td", "th"]):
                # Extract text content
                text = " ".join(td.stripped_strings)
                
                # Extract image URL if present
                img_tag = td.find("img")
                if img_tag:
                    img_url = img_tag["src"]
                    # Append full image URL
                    text = f"{base_url}{img_url}" if img_url.startswith("/") else img_url
                
                cells.append(text)
            if cells:
                rows.append(cells)

        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers[:8])
        return df

    @task
    def transform_data(df):
        # Clean up specific columns
        if 'Stadium' in df.columns:
            df['Stadium'] = df['Stadium'].replace(r'[♦]', '', regex=True)
        if 'Seating capacity' in df.columns:
            df['Seating capacity'] = df['Seating capacity'].replace(r'\[\s*\d+\s*\]', '', regex=True)
        if 'Country' in df.columns:
            df['CountryName'] = df['Country'].str.extract(r'Flag_of_([^/]+)')
            df['CountryName'] = df['CountryName'].str.replace(r'(_|%27|%28|%29)', ' ', regex=True).str.replace('.svg', '', regex=False)
            df['CountryName'] = df['CountryName'].str.replace(r'\s+', ' ', regex=True)
            df['CountryName'] = df['CountryName'].str.replace('converted', '', regex=False)
            df['CountryName'] = df['CountryName'].str.replace('the People s Republic of China', "the People's Republic of China", regex=False)
            df['CountryName'] = df['CountryName'] \
                .str.replace(r'(?i)^the\s', '', regex=True) \
                .str.replace(r'%E2%80%93|%2C|Flag\s+of\s+|[^\w\s]', '', regex=True) \
                .str.replace(r'\s+', ' ', regex=True) \
                .str.replace(r'United Arab Republic.*(Syria)?', r'United Arab Republic, Syria', regex=True)\
                .str.replace(r'United Arab Republic.*(Syria)?', r'United Arab Republic, Syria', regex=True)\
                .str.strip()
        return df

    @task
    def load_data(df):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()

        # Load data into PostgreSQL
        try:
            df.to_sql('stadia_wiki', engine, if_exists='replace', index=False)
            print("Data successfully loaded into 'stadia_wiki'.")
        except Exception as e:
            print(f"Error: {e}")

    load_data(transform_data(get_data(url)))

stadia_pipeline_dag()
