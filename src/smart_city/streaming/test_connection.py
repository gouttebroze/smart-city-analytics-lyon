
import json
import uuid

from confluent_kafka import Producer, Consumer


BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "traffic.raw"


def delivery_report(err, msg):
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

    # 2. Événement de test
    event = {
        "event_type": "traffic.test",
        "source": "smart_city_lyon",
        "message": "Premier événement Redpanda",
        "value": 42
    }

    # 3. Publication
    producer.produce(
        TOPIC,
        key="test-001",
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report
    )

    producer.flush()

    # 4. Création du consommateur
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"smartcity-test-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC])

    try:
        # 5. Lecture du message
        msg = consumer.poll(timeout=10)

        if msg is None:
            print("Aucun message reçu.")
        elif msg.error():
            print(f"Erreur : {msg.error()}")
        else:
            received = json.loads(msg.value().decode("utf-8"))
            print("Message reçu :")
            print(json.dumps(received, indent=4, ensure_ascii=False))

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
