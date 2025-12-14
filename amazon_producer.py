from datasets import load_dataset
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Ambil sample kecil dulu (aman untuk demo & tugas)
dataset = load_dataset(
    "McAuley-Lab/Amazon-Reviews-2023",
    "raw_review_All_Beauty",
    split="full[:300]"
)

for review in dataset:
    producer.send("amazon_reviews", review)

producer.flush()
print("Amazon Reviews berhasil dikirim ke Kafka")
