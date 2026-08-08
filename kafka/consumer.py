from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="instacart-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Waiting for messages...\n")

count = 0

for message in consumer:
    print(message.value)

    count += 1

    if count == 10:
        print("\nDisplayed first 10 messages.")
        break

consumer.close()
