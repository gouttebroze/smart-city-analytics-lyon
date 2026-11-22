
import os

import psycopg
from dotenv import load_dotenv


load_dotenv()

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "smartcity",
    "user": "smartcity",
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def test_database_connection():
    if not DB_CONFIG["password"]:
        raise ValueError("POSTGRES_PASSWORD absent du fichier .env")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            # Vérification de PostgreSQL
            cur.execute("SELECT version();")
            postgres_version = cur.fetchone()[0]

            print("\nConnexion PostgreSQL réussie")
            print(postgres_version)

            # Vérification de PostGIS
            cur.execute("SELECT PostGIS_Full_Version();")
            postgis_version = cur.fetchone()[0]

            print("\nPostGIS disponible")
            print(postgis_version)

            # Vérification des schémas
            cur.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN ('raw', 'staging', 'analytics')
                ORDER BY schema_name;
            """)

            schemas = cur.fetchall()

            print("\nSchémas disponibles :")
            for schema in schemas:
                print(f"- {schema[0]}")


if __name__ == "__main__":
    test_database_connection()
