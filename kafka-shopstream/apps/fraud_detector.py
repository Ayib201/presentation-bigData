"""
fraud_detector.py - Microservice de détection de fraude (groupe "fraud-detection").

Consomme orders.created, applique 3 règles et publie les alertes dans
orders.fraud-alerts. Les messages invalides partent dans orders.dlq.

Sémantique : AT-LEAST-ONCE
  - enable.auto.commit = false ;
  - les offsets ne sont validés qu'APRÈS que les alertes/DLQ ont été
    acquittées par le cluster (producer.flush() puis consumer.commit()).

Plusieurs instances peuvent tourner en parallèle (même group.id) : Kafka
répartit les 6 partitions entre elles (stratégie cooperative-sticky).

Usage :
    python fraud_detector.py --instance A
"""
import argparse
import json
import time
from collections import defaultdict, deque
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from config import BOOTSTRAP, REQUIRED_FIELDS, TOPIC_ALERTS, TOPIC_DLQ, TOPIC_ORDERS

HIGH_AMOUNT = 3000.0          # R1 : montant élevé
GEO_MIN_AMOUNT = 300.0        # R2 : pays carte != pays livraison au-delà de ce montant
VELOCITY_WINDOW_S = 60        # R3 : fenêtre glissante
VELOCITY_MAX = 4              # R3 : nb de commandes déclenchant l'alerte

COMMIT_EVERY_MSGS = 200
COMMIT_EVERY_S = 5.0


def parse_ts(iso):
    return datetime.fromisoformat(iso).timestamp()


class FraudDetector:
    def __init__(self, instance):
        self.instance = instance
        # état local : customer_id -> horodatages des dernières commandes.
        # Correct car la clé = customer_id : un client est toujours lu par
        # la même instance (même partition).
        self.history = defaultdict(deque)
        self.stats = {"lus": 0, "alertes": 0, "dlq": 0}

        self.consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "fraud-detection",
            "client.id": f"fraud-detector-{instance}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        })
        self.producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "client.id": f"fraud-alerts-{instance}",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 10,
        })

    # ---------- callbacks de rééquilibrage ----------
    def on_assign(self, consumer, partitions):
        print(f"[{self.instance}] + partitions ASSIGNÉES : "
              f"{sorted(p.partition for p in partitions)}")

    def on_revoke(self, consumer, partitions):
        print(f"[{self.instance}] - partitions RÉVOQUÉES : "
              f"{sorted(p.partition for p in partitions)}")
        self.checkpoint()   # on valide le travail fait avant de céder les partitions

    # ---------- logique métier ----------
    def evaluate(self, order):
        reasons = []
        amount = float(order["amount"])
        if amount > HIGH_AMOUNT:
            reasons.append(f"HIGH_AMOUNT ({amount:.2f} EUR > {HIGH_AMOUNT:.0f})")
        if order["card_country"] != order["shipping_country"] and amount > GEO_MIN_AMOUNT:
            reasons.append(f"GEO_MISMATCH (carte={order['card_country']}, "
                           f"livraison={order['shipping_country']})")
        ts = parse_ts(order["timestamp"])
        h = self.history[order["customer_id"]]
        h.append(ts)
        while h and ts - h[0] > VELOCITY_WINDOW_S:
            h.popleft()
        if len(h) >= VELOCITY_MAX:
            reasons.append(f"VELOCITY ({len(h)} commandes en {VELOCITY_WINDOW_S}s)")
        return reasons

    def to_dlq(self, msg, error):
        self.producer.produce(
            TOPIC_DLQ, key=msg.key(), value=msg.value(),
            headers=[("error", str(error).encode()),
                     ("source.topic", msg.topic().encode()),
                     ("source.partition", str(msg.partition()).encode()),
                     ("source.offset", str(msg.offset()).encode()),
                     ("detected.by", self.instance.encode())])
        self.stats["dlq"] += 1
        print(f"[{self.instance}] DLQ <- p{msg.partition()}@{msg.offset()} : {error}")

    def handle(self, msg):
        try:
            order = json.loads(msg.value())
            missing = [f for f in REQUIRED_FIELDS if f not in order]
            if missing:
                raise ValueError(f"champs manquants {missing}")
            reasons = self.evaluate(order)
        except (ValueError, TypeError) as e:   # JSONDecodeError hérite de ValueError
            self.to_dlq(msg, e)
            return
        if reasons:
            alert = {
                "alert_id": f"{order['order_id']}-fraud",
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "amount": order["amount"],
                "reasons": reasons,
                "risk_score": min(100, 40 * len(reasons)),
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
                "detected_by": self.instance,
                "detected_at": datetime.now().isoformat(timespec="seconds"),
            }
            self.producer.produce(TOPIC_ALERTS, key=order["customer_id"].encode(),
                                  value=json.dumps(alert).encode())
            self.stats["alertes"] += 1
            print(f"[{self.instance}] ALERTE client={order['customer_id']} "
                  f"montant={order['amount']} -> {' | '.join(reasons)}")
        self.producer.poll(0)

    def checkpoint(self):
        """Durabilité des sorties PUIS validation des offsets (at-least-once)."""
        self.producer.flush(10)
        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException as e:
            # _NO_OFFSET = rien à valider (normal). Les autres erreurs (ex. pendant
            # un rééquilibrage) ne sont pas fatales : les messages seront relus.
            if e.args[0].code() != KafkaError._NO_OFFSET:
                print(f"[{self.instance}] commit non effectué : {e}")

    def run(self):
        self.consumer.subscribe([TOPIC_ORDERS], on_assign=self.on_assign,
                                on_revoke=self.on_revoke)
        print(f"[{self.instance}] démarré, groupe=fraud-detection, topic={TOPIC_ORDERS}")
        since_commit, last_commit, last_report = 0, time.time(), time.time()
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is not None:
                    if msg.error():
                        print(f"[{self.instance}] erreur consommateur : {msg.error()}")
                    else:
                        self.handle(msg)
                        self.stats["lus"] += 1
                        since_commit += 1
                now = time.time()
                if since_commit >= COMMIT_EVERY_MSGS or (
                        since_commit and now - last_commit >= COMMIT_EVERY_S):
                    self.checkpoint()
                    since_commit, last_commit = 0, now
                if now - last_report >= 10:
                    print(f"[{self.instance}] bilan : {self.stats}")
                    last_report = now
        except KeyboardInterrupt:
            print(f"\n[{self.instance}] arrêt...")
        finally:
            self.checkpoint()
            self.consumer.close()   # quitte le groupe proprement -> rééquilibrage immédiat
            print(f"[{self.instance}] bilan final : {self.stats}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--instance", default="A")
    FraudDetector(ap.parse_args().instance).run()
