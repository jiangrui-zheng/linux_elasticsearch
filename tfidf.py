from elasticsearch import Elasticsearch
import pandas as pd

# 初始化 Elasticsearch 客户端
es = Elasticsearch("http://localhost:9200")

# 加载 CSV 文件
df = pd.read_csv('path_to_csv.csv')

# 导入数据到 Elasticsearch
for _, row in df.iterrows():
    doc = {
        "cve": row["cve"],
        "owner": row["owner"],
        "repo": row["repo"],
        "patch": row["patch"]
    }
    es.index(index="linux_commits", body=doc)
