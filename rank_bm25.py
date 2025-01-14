import pandas as pd
import json
from elasticsearch import Elasticsearch
from tqdm import tqdm
import os

cve2desc_path = "/home/jzheng36/code/NVD/cve2desc.json"
valid_list_path = "/home/jzheng36/code/explain_patch/processed_data/valid_list_all_patchfinder.csv"
output_path = "valid_list_ranked.csv"


with open(cve2desc_path, "r") as f:
    cve2desc = json.load(f)
valid_list = pd.read_csv(valid_list_path)
valid_list = valid_list[valid_list['owner'] == 'torvalds']#.iloc[:100]


es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "torvalds@@linux_analyzed"

os.makedirs("ranked_commits", exist_ok=True)

def get_ranked_commit_list(cve_desc, MAX_RANK=100000):
    """
    获取 ranked commit list 并返回 patch 的排名
    :param cve_desc: CVE 描述
    :param patch: 目标补丁 commit ID
    :return: 补丁的排名（如果找到），否则返回 -1
    """
    response = es.search(
        index=INDEX_NAME,
        query={
            "multi_match": {
                "query": cve_desc,
                "fields": ["commit_msg^0.5", "diff^0.3"]
            }
        },
        _source=["commit_id", "datetime"], 
        size=MAX_RANK, # 设置返回的最大文档数
        request_timeout=60 # 设置超时时间
    )
    
    hits = response.get("hits", {}).get("hits", [])
    return [hit["_source"] for hit in hits]

MAX_RANK = 100000 
ranks = []
for _, row in tqdm(valid_list.iterrows(), total=len(valid_list)):
    cve_id = row["cve"]
    patch = row["patch"]
    cve_desc = cve2desc.get(cve_id, [{"lang": "en", "value": ""}])[0]["value"]

    # CVE file
    cve_file_path = os.path.join(f'ranked_commits_{MAX_RANK}', f"{cve_id}.json")
    
    # 加载或生成 ranked commits
    if os.path.exists(cve_file_path) and os.path.getsize(cve_file_path) > 0:
        with open(cve_file_path, "r") as f:
            commits = json.load(f)
    elif cve_desc:
        commits = get_ranked_commit_list(cve_desc, MAX_RANK = MAX_RANK)
        with open(cve_file_path, "w") as f:
            json.dump(commits, f, indent=4)
    else:
        commits = []

    # 获取 patch 的 rank
    for rank, commit in enumerate(commits, start=1):
        # import pdb; pdb.set_trace()
        if commit["commit_id"].strip() == patch.strip():
            ranks.append(rank)
            break
    else:
        ranks.append(-1)  # 如果未找到补丁，则标记为 -1

valid_list["rank"] = ranks
valid_list.to_csv(output_path, index=False)

