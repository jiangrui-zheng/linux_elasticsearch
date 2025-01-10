from elasticsearch import Elasticsearch, helpers
from collections import defaultdict
import json
import math
from tqdm import tqdm


es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "torvalds@@linux_analyzed"

def get_doc_count(index_name):
    """获取索引中的文档总数"""
    response = es.count(index=index_name)
    return response['count']

def calculate_tfidf(field_name, index_name, total_docs):
    term_stats = defaultdict(lambda: {"tf": 0, "doc_freq": 0, "tfidf": 0.0})
    query = {
        "_source": [field_name]
    }
    
    print(f"Calculating TF and Doc_Freq for {field_name}...")
    docs = list(helpers.scan(es, index=index_name, query=query))
    for hit in tqdm(docs, desc=f"Processing {field_name} terms"):
        if field_name in hit["_source"]:
            content = hit["_source"][field_name]
            terms = content.split()
            unique_terms = set(terms)
            
            for term in terms:
                term_stats[term]["tf"] += 1
            for term in unique_terms:
                term_stats[term]["doc_freq"] += 1

    print(f"Calculating IDF and TF-IDF for {field_name}...")
    for term, stats in tqdm(term_stats.items(), desc=f"Calculating TF-IDF for {field_name}"):
        stats["idf"] = math.log((total_docs / (stats["doc_freq"] + 1)))
        stats["tfidf"] = stats["tf"] * stats["idf"]

    return term_stats

def save_to_file(data, file_name):
    with open(file_name, "w") as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    total_docs = get_doc_count(INDEX_NAME)
    print(f"Total documents: {total_docs}")

    # 计算 commit_msg 的 TF-IDF
    print("Calculating TF-IDF for commit_msg...")
    commit_msg_tfidf = calculate_tfidf("commit_msg", INDEX_NAME, total_docs)
    save_to_file(commit_msg_tfidf, "commit_msg_tfidf.json")
    print("Saved commit_msg TF-IDF to commit_msg_tfidf.json")

    # 计算 diff 的 TF-IDF
    print("Calculating TF-IDF for diff...")
    diff_tfidf = calculate_tfidf("diff", INDEX_NAME, total_docs)
    save_to_file(diff_tfidf, "diff_tfidf.json")
    print("Saved diff TF-IDF to diff_tfidf.json")
