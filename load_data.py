from elasticsearch import Elasticsearch, helpers
import json
import logging

logging.basicConfig(level=logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)

# 初始化 Elasticsearch 客户端
es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "01org@@opa-fm"

# 创建索引
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "commit_id": {"type": "keyword"},
                    "datetime": {"type": "date"},
                    "commit_msg": {"type": "text", "analyzer": "standard"},
                    "diff": {"type": "text", "analyzer": "standard"}
                }
            }
        }
    )

# 准备 Bulk 数据
def prepare_bulk_data(json_data, index_name):
    actions = []
    for record in json_data:
        action = {
            "_index": index_name,
            "_source": record
        }
        actions.append(action)
    return actions

# 加载 JSON 数据
# with open("/home/jzheng36/code/linux_elasticsearch/repo2commits_diff/torvalds@@linux.json", "r") as f:
with open("/home/jzheng36/code/linux_elasticsearch/repo2commits_diff/01org@@opa-fm.json", "r") as f:
    data = json.load(f)

# 导入到 Elasticsearch
bulk_data = prepare_bulk_data(data, INDEX_NAME)
helpers.bulk(es, bulk_data)

# 验证导入结果
doc_count = es.count(index=INDEX_NAME)['count']
print(f"Total documents in index '{INDEX_NAME}': {doc_count}")
