"""
revenue_analytics.py - Service d'analytique temps réel (groupe "analytics").

Groupe de consommateurs INDÉPENDANT du groupe fraud-detection : il reçoit
la totalité des commandes (modèle publication/abonnement, fan-out).

Agrège le chiffre d'affaires par fenêtres fixes (tumbling windows) de
WINDOW_S secondes, calées sur l'horodatage métier de la commande (event time).
Une fenêtre est "fermée" puis publiée lorsque le watermark (temps d'événement
minimal parmi les partitions) dépasse sa fin de GRACE_S secondes, ce qui
fonctionne en direct comme lors d'un rejeu de l'historique. Les résultats sont affichés
et ajoutés au fichier revenue_report.csv.

Usage :
    python revenue_analytics.py --window 30
"""
import argparse
import csv
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer

from config import BOOTSTRAP, TOPIC_ORDERS

GRACE_S = 5
REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "revenue_report.csv")


def new_window():
    return {"orders": 0, "revenue": 0.0,
            "by_category": defaultdict(lambda: [0, 0.0]),
            "by_country": defaultdict(float)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=30, help="taille de fenêtre (s)")
    args = ap.parse_args()
    W = args.window

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "analytics",
        "client.id": "revenue-analytics",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,        # simplicité : voir limites dans le rapport
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe([TOPIC_ORDERS])

    windows = defaultdict(new_window)       # window_start (epoch s) -> agrégats
    invalid = late = 0
    part_wm = {}                            # partition -> plus grand horodatage métier vu
    closed_before = 0.0                     # fenêtres antérieures déjà publiées
    last_msg_at = 0.0                       # instant (horloge) du dernier message valide
    new_file = not os.path.exists(REPORT)
    f = open(REPORT, "a", newline="", encoding="utf-8")
    writer = csv.writer(f, delimiter=";")
    if new_file:
        writer.writerow(["window_start_utc", "window_end_utc", "category",
                         "orders", "revenue_eur"])

    print(f"Analytics démarré : fenêtres de {W}s, rapport -> {REPORT}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    o = json.loads(msg.value())
                    ts = datetime.fromisoformat(o["timestamp"]).timestamp()
                    amount = float(o["amount"])
                except (ValueError, TypeError, KeyError):
                    invalid += 1             # ignoré ici (traité par la DLQ de la fraude)
                else:
                    p = msg.partition()
                    part_wm[p] = max(part_wm.get(p, 0.0), ts)
                    last_msg_at = time.time()
                    start = int(ts // W) * W
                    if start < closed_before:
                        late += 1            # fenêtre déjà publiée : événement trop tardif
                    else:
                        w = windows[start]
                        w["orders"] += 1
                        w["revenue"] += amount
                        w["by_category"][o["category"]][0] += 1
                        w["by_category"][o["category"]][1] += amount
                        w["by_country"][o["shipping_country"]] += amount

            # Fermeture des fenêtres pilotée par le WATERMARK = minimum, sur les
            # partitions assignées, du plus grand horodatage vu dans chacune
            # (les partitions avancent à des vitesses différentes, surtout lors
            # d'un rejeu). Si le flux est inactif depuis W + GRACE_S secondes,
            # on se rabat sur l'horloge pour publier la dernière fenêtre.
            now = time.time()
            assigned = {tp.partition for tp in consumer.assignment()}
            if now - last_msg_at >= W + GRACE_S:
                limit = now
            elif assigned and assigned <= part_wm.keys():
                limit = min(part_wm[p] for p in assigned)
            else:
                limit = 0.0                  # toutes les partitions n'ont pas encore parlé
            for start in sorted(list(windows)):
                if start + W + GRACE_S > limit:
                    break
                closed_before = max(closed_before, start + W)
                w = windows.pop(start)
                s = datetime.fromtimestamp(start, timezone.utc)
                e = datetime.fromtimestamp(start + W, timezone.utc)
                print("\n" + "=" * 62)
                print(f" Fenêtre {s:%H:%M:%S} -> {e:%H:%M:%S} UTC | "
                      f"{w['orders']} commandes | CA = {w['revenue']:,.2f} EUR")
                print("-" * 62)
                print(f" {'Catégorie':<14}{'Cmd':>6}{'CA (EUR)':>14}{'Panier moy.':>14}")
                for cat, (n, rev) in sorted(w["by_category"].items(),
                                            key=lambda kv: -kv[1][1]):
                    print(f" {cat:<14}{n:>6}{rev:>14,.2f}{rev / n:>14,.2f}")
                    writer.writerow([s.isoformat(), e.isoformat(), cat, n, round(rev, 2)])
                top = sorted(w["by_country"].items(), key=lambda kv: -kv[1])[:3]
                print(" Top pays livraison : " +
                      ", ".join(f"{c}={v:,.0f}" for c, v in top))
                if invalid or late:
                    print(f" (depuis le démarrage : {invalid} messages invalides ignorés, "
                          f"{late} événements tardifs écartés)")
                f.flush()
    except KeyboardInterrupt:
        print("\nArrêt de l'analytics.")
    finally:
        consumer.close()
        f.close()


if __name__ == "__main__":
    main()
