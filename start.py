from elasticsearch import Elasticsearch
from flask import Flask, request
from urllib.request import urlopen
import json
from sentence_transformers import SentenceTransformer


cloud_id = "My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDZlODAxZjQ5YzAwNDQ5MGRhNDFlOGM3Y2U0MmFmYmQxJGQ2ZTE2YjQ1OWNjMTRhNmZiNDE0ZGZmNmJmN2JjMjll"  # 从 Elastic Cloud 控制台获取
api_key = "TzVkMldKTUJDVjk5bTVXZVFFeGg6LVpiZk04UFZUUE95QXpjbE9VZUttdw=="
url = "https://raw.githubusercontent.com/Kaiyuan-Tan/CSE-512-bonus-point/refs/heads/main/data.json"
model = SentenceTransformer("all-MiniLM-L6-v2")

client = Elasticsearch(
    cloud_id=cloud_id,
    api_key=api_key
)
index_name = "course"
if client.indices.exists(index=index_name):
    client.indices.delete(index=index_name)
if not client.indices.exists(index=index_name):
    mappings = {
        "properties": {
            "title": {
                "type": "keyword",
            },
            "code": {
                "type": "keyword",
            },
            "subject": {
                "type": "keyword",
            }, 
            "description_vector": {
                "type": "dense_vector",
                "dims": 384,
            },
            "instructor": {
                "type": "keyword",
            },    
        }
    }
    response = urlopen(url)
    courses = json.loads(response.read())
    operations = []
    client.indices.create(index=index_name, mappings=mappings)
    for course in courses:
        operations.append({"index": {"_index": index_name}})
        # Transforming the title into an embedding using the model
        course["description_vector"] = model.encode(course["description"]).tolist()
        operations.append(course)
    client.bulk(index=index_name, operations=operations, refresh=True)


def pretty_response(response):
    outputs = []
    if len(response["hits"]["hits"]) == 0:
        print("Your search returned no results.")
    else:
        for hit in response["hits"]["hits"]:
            score = hit["_score"]
            title = hit["_source"]["title"]
            code = hit["_source"]["code"]
            subject = hit["_source"]["subject"]
            description = hit["_source"]["description"]
            instructor = hit["_source"]["instructor"]
            pretty_output = f"Title: {title}; Number: {subject} {code}; Description: {description}; Instructor: {instructor}"
            outputs.append(pretty_output)
    return outputs
app = Flask(__name__)

@app.route("/")
def home():
    try:
        info = client.info()
        print("Connected to Elasticsearch:", info)
    except Exception as e:
        print("Error connecting to Elasticsearch:", e)
    return f"{info}"

@app.route("/search")
def search():
    query = request.args.get("query")  # 获取 'query' 参数
    response = client.search(
        index="course",
        knn={
            "field": "description_vector",
            "query_vector": model.encode(query),
            "k": 10,
            "num_candidates": 100,
        },
    )
    return pretty_response(response)

if __name__ == "__main__":
    app.run(port=8000)
