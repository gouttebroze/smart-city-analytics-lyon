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
                WHEN %s::DOUBLE PRECISION IS NOT NULL
                 AND %s::DOUBLE PRECISION IS NOT NULL
                THEN ST_SetSRID(
                    ST_MakePoint(
                        %s::DOUBLE PRECISION,
                        %s::DOUBLE PRECISION
                    ),
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

    params = (
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
    )

    print("Nombre de paramètres :", len(params))
    print("Valeurs des coordonnées :", longitude, latitude)

    # La connexion et le curseur restent ouverts
    # pendant toute l'exécution de la requête.
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            cur.execute(query, params)

    # Le bloc psycopg.connect valide automatiquement
    # la transaction si aucune exception ne survient.

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
