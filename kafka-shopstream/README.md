# ShopStream — bus d'événements de commandes avec Apache Kafka

Cas pratique du rapport [`Rapport_Kafka_ShopStream.md`](../Rapport_Kafka_ShopStream.md) : un cluster Apache Kafka 4.0 de 3 nœuds (mode KRaft) et trois applications Python illustrant le modèle publication/abonnement.

| Composant | Rôle |
|---|---|
| `docker-compose.yml` | Cluster `kafka1`, `kafka2`, `kafka3` (broker + controller) et Kafka UI (http://localhost:8080) |
| `scripts/create-topics.sh` | Création des topics `orders.created` (6 partitions), `orders.fraud-alerts`, `orders.dlq`, `perf.test` (RF = 3, `min.insync.replicas` = 2) |
| `apps/order_producer.py` | Producteur de commandes (idempotent, `acks=all`, clé = `customer_id`, injection de rafales et de messages corrompus) |
| `apps/fraud_detector.py` | Groupe `fraud-detection` : 3 règles de fraude, alertes, Dead Letter Queue, commit après traitement |
| `apps/revenue_analytics.py` | Groupe `analytics` : chiffre d'affaires par fenêtres de 30 s (watermark par partition) |

## Prérequis

Docker Desktop (4 Go de RAM minimum alloués), Python 3.11 à 3.13, ports 29092, 39092, 49092 et 8080 libres.

## Démarrage rapide (PowerShell)

```powershell
cd kafka-shopstream
docker compose up -d
Get-Content scripts/create-topics.sh -Raw | docker exec -i kafka1 bash

cd apps
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Puis, dans des terminaux séparés (venv activé, dossier `apps`) :

```powershell
python fraud_detector.py --instance A
python revenue_analytics.py --window 30
python order_producer.py --rate 50 --count 6000
```

Sous Linux/macOS : `docker exec -i kafka1 bash < scripts/create-topics.sh` et `source .venv/bin/activate`.

Le protocole complet (montée en charge, pannes de brokers, rejeu, mesures de performance) est décrit dans la partie pratique du rapport.

## Arrêt

```powershell
docker compose down
```
