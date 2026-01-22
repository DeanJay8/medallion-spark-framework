import random
import json
import os
from datetime import datetime

# Use a fixed list of IDs that match your lookup table
VALID_IDS = [101, 102, 103] 

def generate_finance_data(num_records=100):
    output_path = "/Volumes/workspace/lakehouse_db/landing_zone/finance/raw/"
    os.makedirs(output_path, exist_ok=True)
    
    data = []
    for i in range(num_records):
        record = {
            "event_id": f"EVT-{random.randint(1000, 9999)}",
            "source_id": random.choice(VALID_IDS), # Picks from our known IDs
            "amount": round(random.uniform(10.0, 500.0), 2),
            "timestamp": datetime.now().isoformat()
        }
        data.append(record)
    
    file_name = f"batch_{datetime.now().strftime('%H%M%S')}.json"
    with open(f"{output_path}{file_name}", "w") as f:
        json.dump(data, f)
    
    print(f"✅ Generated {num_records} records in {file_name}")