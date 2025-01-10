from datetime import datetime
import os
import json

def convert_datetime_format(data):
    """
    将非标准格式的 datetime 转换为标准 ISO 8601 格式。
    """
    for record in data:
        try:
            original_datetime = record.get("datetime")
            if original_datetime:
                # 转换为 ISO 8601 格式
                parsed_datetime = datetime.strptime(original_datetime, "%a %b %d %H:%M:%S %Y %z")
                record["datetime"] = parsed_datetime.isoformat()  # 转换为 ISO 格式
        except Exception as e:
            print(f"Error converting datetime for record {record.get('commit_id', 'unknown')}: {e}")
    return data

def truncate_record(record, max_record_size):
    """
    截断单条记录的 diff 字段以确保其大小不超过 max_record_size。
    """
    record_size = len(json.dumps(record).encode('utf-8'))
    if record_size > max_record_size:
        diff_length_to_keep = max_record_size - record_size + len(record.get("diff", "").encode('utf-8'))
        if diff_length_to_keep > 0:
            record["diff"] = record["diff"][:diff_length_to_keep]
            print(f"Truncated diff for record {record['commit_id']} to {len(record['diff'])} characters.")
        else:
            record["diff"] = ""
            print(f"Removed diff for record {record['commit_id']} as it exceeds max record size.")

def split_file(input_file, output_dir, max_size=98 * 1024 * 1024, max_record_size=98 * 1024 * 1024):
    """
    将大 JSON 文件拆分为多个小文件，每个文件最大 max_size 字节。
    并将 datetime 转换为标准格式。
    如果单条记录超过 max_record_size，则截断 diff 字段。
    """
    with open(input_file, "r") as f:
        data = json.load(f)

    # 转换 datetime 格式
    data = convert_datetime_format(data)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    base_filename = os.path.splitext(os.path.basename(input_file))[0]

    current_batch = []
    current_size = 0
    file_index = 0

    for record in data:
        # 如果单条记录超出限制，截断 diff
        truncate_record(record, max_record_size)

        record_size = len(json.dumps(record).encode('utf-8'))
        if current_size + record_size > max_size:
            output_file = os.path.join(output_dir, f"{base_filename}_{file_index}.json")
            with open(output_file, "w") as out_f:
                json.dump(current_batch, out_f, indent=4)
            print(f"Saved {len(current_batch)} records to {output_file}")
            current_batch = []
            current_size = 0
            file_index += 1

        current_batch.append(record)
        current_size += record_size

    if current_batch:
        output_file = os.path.join(output_dir, f"{base_filename}_{file_index}.json")
        with open(output_file, "w") as out_f:
            json.dump(current_batch, out_f, indent=4)
        print(f"Saved {len(current_batch)} records to {output_file}")


repo = "torvalds@@linux"
split_file(
    input_file=f"/home/jzheng36/code/linux_elasticsearch/repo2commits_diff/{repo}.json",
    output_dir=f"/home/jzheng36/code/linux_elasticsearch/repo2commits_diff/split_{repo}"
)
