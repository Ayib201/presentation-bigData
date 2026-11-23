#!/bin/bash
# Création des topics ShopStream (exécuté DANS le conteneur kafka1)
# Usage : docker exec -i kafka1 bash < scripts/create-topics.sh
set -e
BIN=/opt/kafka/bin
BS=kafka1:19092

# Flux principal : 6 partitions (parallélisme max = 6 consommateurs par groupe)
$BIN/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic orders.created --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 \
  --config compression.type=producer

# Alertes de fraude produites par le service de détection
$BIN/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic orders.fraud-alerts --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000

# Dead Letter Queue : messages illisibles / invalides
$BIN/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic orders.dlq --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=2592000000

# Topic de banc d'essai pour les tests de performance
$BIN/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic perf.test --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2

echo "=== Topics créés ==="
$BIN/kafka-topics.sh --bootstrap-server $BS --list
