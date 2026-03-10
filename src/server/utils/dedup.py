import logging

import jieba
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

jieba.setLogLevel(logging.WARNING)


def deduplicate_docs(docs: list[dict], threshold: float = 0.75) -> list[list[int]]:
    """
    基于 TF-IDF 余弦相似度对工单文档进行去重分组。
    返回分组列表，每组包含被认为是重复的文档索引。
    """
    texts = [_extract_text(doc) for doc in docs]
    non_empty = [(i, t) for i, t in enumerate(texts) if t.strip()]

    if not non_empty:
        return [[i] for i in range(len(docs))]

    indices, valid_texts = zip(*non_empty)
    tokenized = [" ".join(jieba.cut(t)) for t in valid_texts]

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(tokenized)
    sim_matrix = cosine_similarity(tfidf_matrix)

    n = len(valid_texts)
    visited = [False] * n
    groups: list[list[int]] = []

    for i in range(n):
        if visited[i]:
            continue
        group = [indices[i]]
        visited[i] = True
        for j in range(i + 1, n):
            if not visited[j] and sim_matrix[i][j] >= threshold:
                group.append(indices[j])
                visited[j] = True
        groups.append(group)

    empty_indices = set(range(len(docs))) - set(indices)
    for idx in empty_indices:
        groups.append([idx])

    return groups


def _extract_text(doc: dict) -> str:
    return doc.get("content", {}).get("fields", {}).get("user_content", "")
