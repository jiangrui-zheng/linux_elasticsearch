from elasticsearch import Elasticsearch, helpers
import json
import logging
import time
from tqdm import tqdm
import os

logging.basicConfig(level=logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)

es = Elasticsearch("http://localhost:9200", http_compress=True)
INDEX_NAME = "torvalds@@linux"

# 创建索引（如果尚未存在）
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "commit_id": {"type": "keyword"},
                    "datetime": {"type": "date"},  # 假设 datetime 已是 ISO 8601 格式
                    "commit_msg": {"type": "text", "analyzer": "standard"},
                    "diff": {"type": "text", "analyzer": "standard"}
                    # "diff idx": {"type": "integer"}
                }
            }
        }
    )

def prepare_bulk_data(json_data, index_name, batch_size=100):
    """batch生成 Bulk 数据"""
    for i in range(0, len(json_data), batch_size):
        batch = json_data[i:i + batch_size]
        actions = [
            {"_index": index_name, "_source": record} for record in batch
        ]
        yield actions

def import_split_files(directory, index_name, batch_size=100):
    """
    导入拆分的 JSON 文件到 Elasticsearch
    :param directory: 拆分文件所在目录
    :param index_name: 索引名称
    :param batch_size: 每次导入的批量大小
    """
    files = sorted([os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".json")])
    total_records = 0

    for file in tqdm(files, desc="Processing files"):
        print(f"Processing file: {file}")
        with open(file, "r") as f:
            data = json.load(f)

        for batch in prepare_bulk_data(data, index_name, batch_size=batch_size):
            helpers.bulk(es, batch)
            print(f"Imported {len(batch)} records from {file} into {index_name}.")
            time.sleep(1)  # 可调整，防止过载
        total_records += len(data)

    return total_records




split_directory = f"/home/jzheng36/code/linux_elasticsearch/repo2commits_diff/split_{INDEX_NAME}"
commit2diff_idx = ""
total_records = import_split_files(split_directory, INDEX_NAME, batch_size=5000)


doc_count = es.count(index=INDEX_NAME)['count']
print(f"Total documents in index '{INDEX_NAME}': {doc_count} (Expected: {total_records})")
