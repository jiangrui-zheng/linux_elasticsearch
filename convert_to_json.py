import re
import json
import tqdm
import pandas as pd


commit_pattern = r"Commit: (.+?) Datetime: (.+?)\n([\s\S]*?)(?=\nCommit:|\Z)"
diff_pattern = r"Commit: (.+?) Datetime: (.+?)\n[\s\S]*?(diff --git .+?[\s\S]*?)(?=\nCommit:|\Z)"

def parse_commit_file(file_path, pattern):
    """解析 Commit 文件"""
    with open(file_path, "r", encoding="latin1") as file:
        content = file.read()
    matches = re.findall(pattern, content, re.DOTALL)
    data = {}
    for match in tqdm.tqdm(matches, desc="Parsing commits"):
        commit_hash, datetime, commit_message = match
        data[commit_hash.strip()] = {
            "commit_id": commit_hash.strip(),
            "datetime": datetime.strip(),
            "commit_msg": commit_message.strip(),
            "diff": ""  # 占位，稍后补充 diff
        }
    return data

def parse_diff_file(file_path, pattern):
    """解析 Diff 文件"""
    with open(file_path, "r", encoding="latin1") as file:
        content = file.read()
    matches = re.findall(pattern, content, re.DOTALL)
    data = {}
    for match in tqdm.tqdm(matches, desc="Parsing diffs"):
        commit_hash, datetime, diff_content = match
        data[commit_hash.strip()] = {
            "commit_id": commit_hash.strip(),
            "datetime": datetime.strip(),
            "diff": diff_content.strip()
        }
    return data

def merge_data(commits, diffs):
    """合并 Commit 和 Diff 数据"""
    commit_ids = set(commits.keys())  # 提前转为 set
    for commit_id, diff_data in diffs.items():
        if commit_id in commit_ids:
            commits[commit_id]["diff"] = diff_data["diff"]
        else:
            commits[commit_id] = diff_data  # 保留 diff 数据
    return list(commits.values())

def save_to_json(data, output_path):
    """保存数据为 JSON 文件"""
    with open(output_path, "w") as file:
        json.dump(data, file, indent=4)

def process_repo(owner_repo, base_path="/home/jzheng36/code"):
    """
    处理指定仓库的 Commit 和 Diff 数据，并保存为 JSON 文件

    :param owner_repo: 形如 "owner@@repo" 的仓库标识
    :param base_path: 基础路径
    """
    commit_file = f"{base_path}/explain_patch/data/commits/commits_{owner_repo}.txt"
    diff_file = f"{base_path}/explain_patch/data/diff/diff_{owner_repo}.txt"
    output_file = f"{base_path}/linux_elasticsearch/repo2commits_diff/{owner_repo}.json"

    print(f"Processing repository: {owner_repo}")

    # 解析 Commit 和 Diff 文件
    commits_data = parse_commit_file(commit_file, commit_pattern)
    diffs_data = parse_diff_file(diff_file, diff_pattern)

    # 合并数据
    merged_data = merge_data(commits_data, diffs_data)

    # 保存为 JSON 文件
    save_to_json(merged_data, output_file)
    print(f"Parsed {len(merged_data)} commits and saved to {output_file}")


if __name__ == "__main__":
    # df = pd.read_csv('/home/jzheng36/code/explain_patch/processed_data/valid_list_all_patchfinder.csv')#.iloc[:10]
    # df = df[df['owner'] != 'torvalds']
    # repos = df['owner'] + '@@' + df['repo']
    # for repo in repos:
    #     process_repo(repo)
    process_repo("torvalds@@linux")
