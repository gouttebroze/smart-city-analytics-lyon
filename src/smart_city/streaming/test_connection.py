
import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "traffic.raw"


def delivery_report(err, msg):
    """Affiche le résultat de la publication Kafka."""

    if err is not None:
        print(f"Erreur de livraison : {err}")
    else:
        print(
            f"Message envoyé : "
            f"topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


def main():

    # 1. Création du producteur
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS
    })

    # 2. Identifiant unique de l'événement
    event_id = str(uuid.uuid4())

    # 3. Création de l'événement géolocalisé
    event = {
        "event_id": event_id,
        "event_type": "traffic.test",
        "source": "smart_city_lyon",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": 45.764043,
        "longitude": 4.835659,
        "message": "Test événement géolocalisé Lyon",
        "value": 42
    }

    # 4. Publication dans Redpanda
    try:
        producer.produce(
            TOPIC,
            key=event_id,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )

        remaining = producer.flush(timeout=10)

        if remaining > 0:
            raise RuntimeError(
                f"{remaining} message(s) encore en attente de livraison"
            )

        print("\nÉvénement publié :")
        print(json.dumps(event, indent=4, ensure_ascii=False))

    except Exception as exc:
        print(f"Erreur lors de la publication : {exc}")
        raise


if __name__ == "__main__":
    main()
