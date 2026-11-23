"""
order_producer.py - Simule le service "Commandes" d'une plateforme e-commerce.

Publie des événements OrderCreated (JSON) dans le topic orders.created.
 - clé du message = customer_id  -> toutes les commandes d'un client vont dans
   la même partition (ordre garanti par client) ;
 - producteur idempotent, acks=all  -> pas de perte ni de doublon côté broker ;
 - injecte volontairement des rafales (fraude "vélocité") et des messages
   corrompus (pour démontrer la Dead Letter Queue).

Usage :
    python order_producer.py --rate 50 --count 5000
    python order_producer.py --rate 200            # infini, Ctrl+C pour arrêter
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

from config import BOOTSTRAP, TOPIC_ORDERS

# catégorie -> (prix unitaire min, prix unitaire max)
CATEGORIES = {
    "electronique": (40.0, 2400.0),
    "mode": (8.0, 250.0),
    "maison": (12.0, 900.0),
    "livres": (4.0, 55.0),
    "sport": (10.0, 600.0),
}
CATEGORY_WEIGHTS = [0.25, 0.30, 0.20, 0.10, 0.15]
COUNTRIES = ["FR", "BE", "CH", "MA", "SN", "CA", "TN", "CI"]
COUNTRY_WEIGHTS = [0.30, 0.12, 0.08, 0.18, 0.08, 0.10, 0.08, 0.06]
CHANNELS = ["web", "mobile", "marketplace"]
PAYMENTS = ["card", "card", "card", "paypal", "wallet"]
CUSTOMERS = [f"C{n:05d}" for n in range(1, 50001)]

stats = {"sent": 0, "ok": 0, "errors": 0}
partition_hist = {}


def make_order(customer_id=None):
    category = random.choices(list(CATEGORIES), weights=CATEGORY_WEIGHTS)[0]
    lo, hi = CATEGORIES[category]
    unit_price = round(random.uniform(lo, hi), 2)
    quantity = random.choices([1, 2, 3, 4], weights=[0.70, 0.18, 0.08, 0.04])[0]
    shipping = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
    # ~5 % de cartes émises dans un autre pays que le pays de livraison
    card = shipping if random.random() > 0.05 else random.choice(COUNTRIES)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": customer_id or random.choice(CUSTOMERS),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "category": category,
        "product_id": f"P-{category[:4].upper()}-{random.randint(1, 400):04d}",
        "quantity": quantity,
        "unit_price": unit_price,
        "amount": round(unit_price * quantity, 2),
        "currency": "EUR",
        "payment_method": random.choice(PAYMENTS),
        "card_country": card,
        "shipping_country": shipping,
        "channel": random.choice(CHANNELS),
    }


def on_delivery(err, msg):
    """Callback appelé de façon asynchrone pour chaque message (accusé broker)."""
    if err is not None:
        stats["errors"] += 1
        print(f"[ERREUR] livraison échouée : {err}")
        return
    stats["ok"] += 1
    p = msg.partition()
    partition_hist[p] = partition_hist.get(p, 0) + 1


def send(producer, key, value_bytes):
    while True:
        try:
            producer.produce(TOPIC_ORDERS, key=key, value=value_bytes,
                             on_delivery=on_delivery)
            stats["sent"] += 1
            break
        except BufferError:
            # file d'attente locale pleine : on laisse librdkafka évacuer
            producer.poll(0.5)
    producer.poll(0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=50, help="commandes / seconde")
    ap.add_argument("--count", type=int, default=0, help="0 = infini")
    ap.add_argument("--malformed", type=float, default=0.01,
                    help="proportion de messages corrompus (DLQ)")
    ap.add_argument("--burst", type=float, default=0.01,
                    help="probabilité de rafale de commandes (fraude vélocité)")
    args = ap.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "order-service",
        "enable.idempotence": True,      # implique acks=all, retries>0, in-flight<=5
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,                 # attendre 20 ms pour remplir les lots
        "batch.size": 65536,
        "delivery.timeout.ms": 120000,
    })

    print(f"Producteur connecté à {BOOTSTRAP} -> topic {TOPIC_ORDERS}")
    delay = 1.0 / args.rate if args.rate > 0 else 0
    t0 = time.time()
    last_report = t0
    try:
        while args.count == 0 or stats["sent"] < args.count:
            r = random.random()
            if r < args.malformed:
                # message corrompu : JSON tronqué
                send(producer, b"CORRUPTED", b'{"order_id": "broken", "amount": ')
            elif r < args.malformed + args.burst:
                # rafale : 5 commandes du même client en moins d'une seconde
                cid = random.choice(CUSTOMERS)
                for _ in range(5):
                    o = make_order(cid)
                    send(producer, cid.encode(), json.dumps(o).encode())
            else:
                o = make_order()
                send(producer, o["customer_id"].encode(), json.dumps(o).encode())

            if delay:
                time.sleep(delay)
            now = time.time()
            if now - last_report >= 5:
                rate = stats["ok"] / (now - t0)
                print(f"[{datetime.now():%H:%M:%S}] envoyés={stats['sent']} "
                      f"acquittés={stats['ok']} erreurs={stats['errors']} "
                      f"débit_moyen={rate:.1f} msg/s")
                last_report = now
    except KeyboardInterrupt:
        print("\nArrêt demandé...")
    except KafkaException as e:
        print(f"Erreur Kafka : {e}")
    finally:
        remaining = producer.flush(30)
        elapsed = time.time() - t0
        print("=" * 60)
        print(f"Total envoyés : {stats['sent']}  | acquittés : {stats['ok']}  "
              f"| erreurs : {stats['errors']}  | non livrés : {remaining}")
        print(f"Durée : {elapsed:.1f}s  | débit moyen : {stats['ok'] / max(elapsed, 1e-9):.1f} msg/s")
        print("Répartition par partition :",
              dict(sorted(partition_hist.items())))


if __name__ == "__main__":
    main()
