# 2 step — Redpanda vers PostgreSQL/PostGIS

* Après que les 2 tests de validation suivant soit passés, soit:
    - **Redpanda ↔ Python** (*publication et lecture d'un événement JSON réussies*).
- **Python ↔ PostgreSQL/PostGIS** *(connexion réussie et présence des schémas **raw**, **staging** et **analytics**)*.

> **Objectif**: faire transiter un message Redpanda jusqu'à PostgreSQL

> Mise en place des tests pemettant de distinguer les **problèmes d'infrastructure** des problèmes liés aux **données open data** avant de **connecter l'API réelle du trafic lyonnais**.

1. **Création de la table de réception des événements bruts**

Pr stocker :
- les **métadonnées Kafka**(topic, partition, offset)
- le **type** d'événement et sa source
- le **JSON original** dans une **colonne JSONB**
- l'heure de réception
- une **géométrie PostGIS** facultative, lorsque les **coordonnées seront disponibles**.

* création fichier [init.sql](../../docker/postgre/init.sql)
```sql

CREATE TABLE IF NOT EXISTS raw.traffic_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    event_type TEXT NOT NULL,
    source TEXT NOT NULL,

    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_timestamp TIMESTAMPTZ,

    payload JSONB NOT NULL,

    geom geometry(Point, 4326),

    CONSTRAINT uq_traffic_kafka_message
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_traffic_events_collected_at
    ON raw.traffic_events (collected_at);

CREATE INDEX IF NOT EXISTS idx_traffic_events_geom
    ON raw.traffic_events
    USING GIST (geom);
```
* détail structure ``SQL`` ?

La **contrainte unique sur le topic, la partition et l'offset** évite d'enregistrer deux fois le même message Kafka si le consommateur doit le **retraiter après une interruption**.


* `payload JSONB` conserve le contenu original sans imposer dès maintenant un schéma métier définitif.
* ``geom`` sera renseigné lorsque les événements contiendront des coordonnées valides.
* Application du script SQL, depuis PowerShell, à la racine du projet :

```sh
Get-Content .\docker\postgres\schema.sql |
    docker exec -i smartcity_postgres psql -U smartcity -d smartcity
```

* Test pr vérifier la création de la table 
```sh
docker exec -it smartcity_postgres psql -U smartcity -d smartcity -c "\d raw.traffic_events"
```

### Création du **consommateur Redpanda** vers **PostgreSQL**

* Création & implémentation du fichier [consumer_to_postgres.py](./src/smart_city/streaming/consumer_to_postgres.py)

    - Ce **consommateur** va :
        1. lire les événements de traffic.raw 
        2. décoder le JSON 
        3. récupérer les champs principaux 
        4. insérer le message dans PostgreSQL 
        5. valider l'offset Kafka seulement après l'insertion réussie
        
```py

import json
import os
from datetime import datetime

import psycopg
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv


load_dotenv()

TOPIC = "traffic.raw"

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "smartcity-postgres-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "smartcity",
    "user": "smartcity",
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def parse_timestamp(value):
    """Convertit une date ISO 8601 en datetime Python."""
    if not value:
        return None

    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def extract_coordinates(event):
    """Retourne longitude et latitude si elles sont valides."""
    latitude = event.get("latitude")
    longitude = event.get("longitude")

    if latitude is None or longitude is None:
        return None, None

    latitude = float(latitude)
    longitude = float(longitude)

    if not (-90 <= latitude <= 90):
        raise ValueError(f"Latitude invalide : {latitude}")

    if not (-180 <= longitude <= 180):
        raise ValueError(f"Longitude invalide : {longitude}")

    return longitude, latitude


def insert_event(message, event):
    """Insère un événement Kafka dans PostgreSQL/PostGIS."""

    event_type = event.get("event_type", "unknown")
    source = event.get("source", "unknown")

    if not isinstance(event_type, str) or not isinstance(source, str):
        raise ValueError("event_type et source doivent être des chaînes")

    longitude, latitude = extract_coordinates(event)

    event_timestamp = parse_timestamp(
        event.get("event_timestamp")
    )

    payload = json.dumps(event, ensure_ascii=False)

    query = """
        INSERT INTO raw.traffic_events (
            kafka_topic,
            kafka_partition,
            kafka_offset,
            event_type,
            source,
            event_timestamp,
            payload,
            geom
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s::jsonb,
            CASE
                WHEN %s IS NOT NULL AND %s IS NOT NULL
                THEN ST_SetSRID(
                    ST_MakePoint(%s, %s),
                    4326
                )
                ELSE NULL
            END
        )
        ON CONFLICT (
            kafka_topic,
            kafka_partition,
            kafka_offset
        ) DO NOTHING;
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    event_type,
                    source,
                    event_timestamp,
                    payload,
                    longitude,
                    latitude,
                    longitude,
                    latitude,
                ),
            )


def main():
    if not DB_CONFIG["password"]:
        raise ValueError("POSTGRES_PASSWORD absent du fichier .env")

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    print(f"Écoute du topic : {TOPIC}")
    print("Appuyer sur Ctrl+C pour arrêter.")

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            event = json.loads(message.value().decode("utf-8"))

            # L'insertion doit réussir avant la validation Kafka.
            insert_event(message, event)

            consumer.commit(
                message=message,
                asynchronous=False
            )

            print(
                "Événement enregistré : "
                f"partition={message.partition()}, "
                f"offset={message.offset()}"
            )

    except KeyboardInterrupt:
        print("\nArrêt demandé.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

```

> Le consommateur conserve volontairement une stratégie simple : si l'insertion échoue, il s'arrête sans valider l'offset du message. Cela évite de considérer comme traité un événement qui n'a pas été enregistré.

### Exécution du pipeline de test

* Ouvrir deux terminaux PowerShell.

- **Terminal 1** — lancer le consommateur :

```sh
.\.venv\Scripts\Activate.ps1 # si l'environnement virtuel n'est pas lancé
python -m smart_city.streaming.consumer_to_postgres

# Résultats attendus:
# Écoute du topic : traffic.raw
# Nombre de paramètres : 11
# Valeurs des coordonnées : None None
# Événement enregistré : partition=0, offset=...
```

- **deuxième terminal** - lancer le producteur de test :

```sh
# sous un environnement virtuel
python -m smart_city.streaming.test_connection
```

---
* **ordre attendu** :
    1. Réception du message Kafka.
    2. Insertion dans PostgreSQL.
    3. Validation de la transaction PostgreSQL.
    4. Validation de l'offset Kafka.

* voici un bloc de code extrait de la fonction `.main()` du fichier `./streaming/consumer_to_postgry.py` qui respecte un principe essentiel:

    - ***Un message Kafka ne doit pas être valider avant que son insertion en base soit réussie***

```py
# L'insertion doit réussir avant la validation Kafka.
insert_event(message, event)

consumer.commit(
    message=message,
    asynchronous=False
)
```

* **TESTS**: Vérifier l'**insertion** dans **PostgreSQL**

* on lance `psql` dans notre terminal:
```sh
docker exec -it smartcity_postgres psql -U smartcity -d smartcity
```

* et on pt lancer la requete SQL suivante:
```sql
SELECT
    id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    event_type,
    source,
    event_timestamp,
    payload,
    ST_AsText(geom) AS geom
FROM raw.traffic_events
ORDER BY id DESC
LIMIT 10;
```

**! TODO: AFFICHER L'IMAGE du RESULTAT DE LA REQUETE**

### Cycle de vie attendu

1. Connexion à PostgreSQL

2. Écoute du topic traffic.raw

3. Réception et décodage du message JSON

4. Insertion dans raw.traffic_events

5. Validation de l'offset Kafka

6. Retour à l'écoute du prochain message

* La connexion PostgreSQL doit rester ouverte pendant toute la boucle de consommation et être fermée proprement à l'arrêt du programme.

---
--- 
## Etape 5 / 6 (sur objectifs d'avancement définis au 17/11/26) -> Tester la création et stockage d'une géométrie PostGIS

### Vérification de l'idempotence

- avec **contrainte d'unicité**:
```sql
UNIQUE (
    kafka_topic,
    kafka_partition,
    kafka_offset
)
```

- & une **instruction** :
```sql
ON CONFLICT (
    kafka_topic,
    kafka_partition,
    kafka_offset
) DO NOTHING
```

> protège PostgreSQL contre la **réinsertion du même message Kafka**.

### Vérification des offsets enregistrés:

* lancement `PSQL` ds terminal:
```sh
docker exec -it smartcity_postgres psql -U smartcity -d smartcity
```
* requete `PSQL`
```sql
SELECT
    kafka_topic,
    kafka_partition,
    COUNT(*) AS nombre_evenements,
    COUNT(DISTINCT kafka_offset) AS offsets_distincts
FROM raw.traffic_events
GROUP BY
    kafka_topic,
    kafka_partition;
```
- On attent que les deux compteurs doivent être égaux.

- résultat:

| kafka_topic | kafka_partition | nombre_evenements | offsets_distincts 
|:-----------:|:---------------:|:-----------:|:---------------:|
| traffic.raw |               0 |                 4 |                 4
(1 row)

---
### Tests des coordonnées géographiques

**Deuxième test avec un événement contenant des coordonnées GPS**

* **résultat**:

```sh
Message envoyé : topic=traffic.raw, partition=0, offset=4

Événement publié :
{
    "event_id": "cad9ec2c-a7d8-4ebe-9891-9843cac2a7d6",
    "event_type": "traffic.test",
    "source": "smart_city_lyon",
    "event_timestamp": "2026-11-21T00:07:22.832860+00:00",
    "latitude": 45.764043,
    "longitude": 4.835659,
    "message": "Test événement géolocalisé Lyon",
    "value": 42
}
```
---

Le test permet de vérifier trois éléments supplémentaires :
1. Le transport d'un événement JSON géolocalisé via Redpanda.
2. La conversion des coordonnées GPS en géométrie PostGIS.
3. La conservation de l'horodatage dans PostgreSQL.

```
Python Producer
      |
      v
Redpanda (traffic.raw)
      |
      v
Python Consumer
      |
      v
PostgreSQL / PostGIS
      |
      v
raw.traffic_events
```

* **Interprétation du résultat**

Élément	Résultat	État
Géométrie	POINT(4.835659 45.764043)	Validé
SRID	4326	Validé
Horodatage dans le JSON	2026-11-21T00:07:22.832860+00:00	Présent
Coordonnées	Longitude 4.835659, latitude 45.764043	Validées

Le SRID 4326 correspond au système de coordonnées géographiques WGS 84, couramment utilisé pour les coordonnées GPS.

La géométrie est bien construite dans le bon ordre : longitude, puis latitude.

2. **Vérification complémentaire de la colonne event_timestamp**

La requête suivante permet de vérifier que l'horodatage est également enregistré dans sa colonne dédiée:

```sql
SELECT
    id,
    event_type,
    event_timestamp,
    payload ->> 'event_timestamp' AS timestamp_json,
    ST_X(geom) AS longitude,
    ST_Y(geom) AS latitude,
    ST_SRID(geom) AS srid
FROM raw.traffic_events
ORDER BY id DESC
LIMIT 5;
```

On pt comparer l'horodatage extrait du JSON avec celui stocké dans la colonne SQL.

3. **Bilan du pipeline**

Smart City Analytics – Lyon

1. Producteur Python
   Validé
   
   Publication d'événements JSON.
2. Redpanda
   Validé
   
   Transport via traffic.raw.
3. Consommateur Python
   Validé
   
   Lecture, décodage et traitement des événements.
4. PostgreSQL
   Validé
   
   Persistance des événements en JSONB.
5. PostGIS
   Validé
   
   Création de géométries POINT en SRID 4326.