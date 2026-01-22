import os, json, random
from datetime import datetime, timedelta
from medallion_backbone.config import BASE_PATH

DOMAIN = "taxi"
OUTPUT_DIR = f"{BASE_PATH}/{DOMAIN}/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_RECORDS = 50
START_DATE = datetime(2026, 1, 1)

records = []
for i in range(NUM_RECORDS):
    records.append({
        "ride_id": i + 1,
        "passenger_count": random.randint(1, 4),
        "trip_distance": round(random.uniform(0.5, 15), 2),
        "pickup_datetime": (START_DATE + timedelta(minutes=random.randint(0, 14400))).isoformat(),
        "dropoff_datetime": (START_DATE + timedelta(minutes=random.randint(10, 15000))).isoformat(),
        "payment_type": random.choice(["cash", "card"]),
        "fare_amount": round(random.uniform(5, 50), 2)
    })

# Write JSON
output_file = os.path.join(OUTPUT_DIR, f"taxi_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
with open(output_file, "w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

print(f"Generated {NUM_RECORDS} taxi records → {output_file}")
