import pandas as pd
import json
from elasticsearch import Elasticsearch
from tqdm import tqdm
import os


def generate_features(cve_desc, commit, patch):
    """
    为每个候选补丁生成特征
    :param cve_desc: CVE 描述
    :param commit: 候选补丁
    :param patch: 真实补丁
    :return: 特征字典
    """
    features = {
        "bm25_commit_msg": commit.get("bm25_commit_msg", 0),
        "bm25_diff": commit.get("bm25_diff", 0), 
        "doc_length" : 0.7 * len(commit["commit_msg"].split()) + 0.3 * len(commit["diff"].split()),
        "is_patch": 1 if commit["commit_id"] == patch else 0 
    }
    return features
