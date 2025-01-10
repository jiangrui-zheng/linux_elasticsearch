import requests
import json

es_url = "http://localhost:9200/_reindex?scroll=5m"
source_index = "torvalds@@linux"
dest_index = "torvalds@@linux_analyzed"
max_slices = 100

for slice_id in range(max_slices):
    payload = {
        "source": {
            "index": source_index,
            "slice": {
                "id": slice_id,
                "max": max_slices
            }
        },
        "dest": {
            "index": dest_index
        }
    }
    response = requests.post(es_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
    print(f"Slice {slice_id} response: {response.json()}")
