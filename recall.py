import os
import json
import pandas as pd
from tqdm import tqdm


valid_list_path = "/home/jzheng36/code/explain_patch/processed_data/valid_list_all_patchfinder.csv"
ranked_commits_dir = "/home/jzheng36/code/linux_elasticsearch/ranked_commits"

valid_list = pd.read_csv(valid_list_path)
valid_list = valid_list[valid_list['owner'] == 'torvalds']

k_values = range(1000, 5001, 1000)

def calculate_recall_at_k(valid_list, ranked_commits_dir, k_values):
    """
    计算 recall@k
    :param valid_list: 包含真实 patch 信息的 DataFrame
    :param ranked_commits_dir: 存储每个 CVE ranked commits 的目录
    :param k_values: 要计算的 k 值列表
    :return: 包含每个 k 值对应 recall 的结果字典
    """
    recall_results = {k: 0 for k in k_values}
    total_cves = 0

    for _, row in tqdm(valid_list.iterrows(), total=len(valid_list)):
        cve_id = row["cve"]
        patch = row["patch"]
        cve_file_path = os.path.join(ranked_commits_dir, f"{cve_id}.json")
        
        if not os.path.exists(cve_file_path):  # 如果文件不存在，跳过
            continue
        
        with open(cve_file_path, "r") as f:
            ranked_commits = json.load(f)
        
        # 提取 commit_id list
        ranked_commit_ids = [item["commit_id"] for item in ranked_commits]
        
        # recall@k
        for k in k_values:
            if patch in ranked_commit_ids[:k]:  # 如果真实补丁出现在前 k 个
                recall_results[k] += 1
        
        total_cves += 1  # 总的 CVE 数量

    # 计算 recall 比例
    recall_results = {k: recall_results[k] / total_cves for k in k_values}
    return recall_results


recall_at_k_results = calculate_recall_at_k(valid_list, ranked_commits_dir, k_values)
print("Recall@k Results:")
for k, recall in recall_at_k_results.items():
    print(f"Recall@{k}: {recall:.4f}")

output_path = "recall_at_k_results.json"
with open(output_path, "w") as f:
    json.dump(recall_at_k_results, f, indent=4)
print(f"Results saved to {output_path}")
