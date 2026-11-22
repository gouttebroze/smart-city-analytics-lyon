> Step développement local 

> Réalisation au 14/11/2026

# Suivi hebdomadaire – Smart City Analytics · Lyon

1. Objectifs et périmètre

> Construction projet Data Analytics orienté Smart City, prenant Lyon comme territoire d'expérimentation.

* Les **objectifs** sont de :

    - Collecter des données urbaines en temps réel ou quasi réel.
    - Construire une chaîne d'ingestion et de traitement avec Python et Redpanda.
    - Stocker les données dans PostgreSQL/PostGIS.
    - Réaliser des analyses temporelles et géospatiales.
    - Produire des indicateurs exploitables dans un tableau de bord.
    - Préparer ultérieurement une application Android.

## Etat des issues: 

* Ttes faites sauf la connection Python à PostgreSQL/PostGIS

* Validation du premier échange de données ds  l'architecture Smart City.

|Composant|	Résultat|
|:--------|:--------|
|Docker Compose |	Validé|
|PostgreSQL |	Healthy|
|PostGIS 3.5.2  |	Validé|
|Package Python |	Validé|
|Topic traffic.raw  |	Validé|
|Producer Python    |	Validé|
|Consumer Python    |	Validé|
|Insertion PostgreSQL depuis Python |	À tester|

> **Test sur Streaming traffic connection**: L'**offset 1** indique que le **message** a été enregistré à la **position 1 de sa partition**, ça ne représente pas une erreur.

### Connection Python à PostgreSQL/PostGIS

> validation du 2e échange Python → PostgreSQL → PostGIS

+ **objectif**: vérifier que **Python** peut se **connecter** à la base, exécuter des **requêtes SQL** (**PGSQL**) et récupérer les **informations spatiales** (**PostGIS**).

+ **test de connection**

```py
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

```

* Test de cette connection
```sh
python -m src.smart_city.database.test_connection

# ce test de connection ns retourne ces 3 informations: 
# 
#   +- Connexion PostgreSQL réussie 
#   +- PostGIS disponible 
#   +- Schémas disponibles :
#       - analytics
#       - raw
#       - staging
```

## RESUME SUIVI au 14/11/26 

### Mise en place de l'architecture de l'application
### FLUX DE DONNEES - Configuration, Tests & Validation 

* **validation** suivantes (environnement local) :
    - **Redpanda** ↔ **Python** : **publication** et **lecture** d'un **événement JSON** réussies.
    - **Python** ↔ **PostgreSQL/PostGIS** : **connexion réussie** et présence des schémas: 
        + ``raw``
        + ``staging`` 
        + ``analytics``