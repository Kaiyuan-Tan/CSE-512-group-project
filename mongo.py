from pymongo import MongoClient
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from urllib.request import urlopen
import json
from sentence_transformers import SentenceTransformer

cloud_id = "My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDZlODAxZjQ5YzAwNDQ5MGRhNDFlOGM3Y2U0MmFmYmQxJGQ2ZTE2YjQ1OWNjMTRhNmZiNDE0ZGZmNmJmN2JjMjll"  # 从 Elastic Cloud 控制台获取
api_key = "TzVkMldKTUJDVjk5bTVXZVFFeGg6LVpiZk04UFZUUE95QXpjbE9VZUttdw=="
model = SentenceTransformer("all-MiniLM-L6-v2")
url = "mongodb+srv://ktan24:mongodb@cluster0.t9a5q.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
data_url = "https://raw.githubusercontent.com/Kaiyuan-Tan/CSE-512-group-project/refs/heads/main/data.json"

DB_NAME = "project"
COLLECTION_NAME = "user"
INDEX_NAME = "books"

class AtlasClient:

    def __init__(self, altas_uri, dbname):
        self.mongodb_client = MongoClient(altas_uri)
        self.database = self.mongodb_client[dbname]

    # A quick way to test if we can connect to Atlas instance
    def ping(self):
        self.mongodb_client.admin.command("ping")

    # Get the MongoDB Atlas collection to connect to
    def get_collection(self, collection_name):
        collection = self.database[collection_name]
        return collection

    # Query a MongoDB collection
    def find(self, collection_name, filter={}, limit=0):
        collection = self.database[collection_name]
        items = list(collection.find(filter=filter, limit=limit))
        return items
    
    def insert(self, collection_name, user_info):
        collection = self.database[collection_name]
        result = collection.insert_one(user_info)
        return result

    def delete(self, collection_name, user_id):
        collection = self.database[collection_name]
        result = collection.delete_one(user_id)
        return result

app = Flask(__name__)

atlas_client = AtlasClient(url, DB_NAME)
collection = atlas_client.get_collection(COLLECTION_NAME)
app.secret_key = "CSE-512-GROUP-PROJECT-2024"
client = Elasticsearch(
    cloud_id=cloud_id,
    api_key=api_key
)
if client.indices.exists(index=INDEX_NAME):
    client.indices.delete(index=INDEX_NAME)
if not client.indices.exists(index=INDEX_NAME):
    mappings = {
        "properties": {
            "title": {
                "type": "keyword",
            },
            "author": {
                "type": "keyword",
            },
            "genre": {
                "type": "keyword",
            }, 
            "summary_vector": {
                "type": "dense_vector",
                "dims": 384,
            },
            "ISBN_13": {
                "type": "keyword",
            },
            "publisher": {
                "type": "keyword",
            },
            "publication_date": {
                "type": "date",
                "format": "yyyy-MM-dd||yyyy-M-d||epoch_millis"
            },
            "search_times": {
                "type": "integer",
            },
        }
    }
    response = urlopen(data_url)
    books = json.loads(response.read())
    operations = []
    client.indices.create(index=INDEX_NAME, mappings=mappings)
    for book in books:
        operations.append({"index": {"_index": INDEX_NAME}})
        book["summary_vector"] = model.encode(book["summary"]).tolist()
        operations.append(book)
    result = client.bulk(index=INDEX_NAME, operations=operations, refresh=True)
    # count = client.count(index=INDEX_NAME)
    print(result)

def pretty_response(response):
    outputs = []
    if len(response["hits"]["hits"]) == 0:
        print("Your search returned no results.")
    else:
        for hit in response["hits"]["hits"]:
            output = {
                "id": hit["_id"],
                "score": hit["_score"],
                "title": hit["_source"]["title"],
                "date": hit["_source"]["publication_date"],
                "publisher": hit["_source"]["publisher"],
                "search_times": hit["_source"]["search_times"],
                "author": hit["_source"]["author"],
                "isbn": hit["_source"]["ISBN-13"],
                "genre": hit["_source"]["genre"],
                "summary": hit["_source"]["summary"]
            }
            outputs.append(output)
    return outputs

def search_time_increase(response):
    for resp in response:
        update_body = {
            "script": {
                "source": "ctx._source.search_times += 1",
                "lang": "painless"
            }
        }
        result = client.update(index=INDEX_NAME, id=resp["id"], body=update_body)
        # print(result)

# Create account
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data['username']
    email = data['email']
    user_info = {
        "username": username,
        "email": email,
        "search_history":[]
    }

    if atlas_client.find(collection_name=COLLECTION_NAME, filter={"email": email}):
        return jsonify({"message": "Email already exists"}), 400

    resp = atlas_client.insert(collection_name=COLLECTION_NAME, user_info=user_info)
    return jsonify({"message": f"User registered successfully, id: {resp.inserted_id}"}), 201

# Delete account
@app.route('/delete', methods=['POST'])
def delete():
    data = request.json
    email = data['email']

    if not atlas_client.find(collection_name=COLLECTION_NAME, filter={"email": email}):
        return jsonify({"message": "Email do not exist"}), 400

    resp = atlas_client.delete(collection_name=COLLECTION_NAME, user_id={"email": email})
    return jsonify({"message": f"User deleted successfully. See you again"}), 202

# Elastic Search Home
@app.route("/elasticsearch")
def home():
    try:
        info = client.info()
        print("Connected to Elasticsearch:", info)
    except Exception as e:
        print("Error connecting to Elasticsearch:", e)
    return f"{info}"

# Search by summary
@app.route("/elasticsearch/summary")
def search():
    query = request.args.get("query") 
    response = client.search(
        index=INDEX_NAME,
        knn={
            "field": "summary_vector",
            "query_vector": model.encode(query),
            "k": 10,
            "num_candidates": 100,
        },
    )
    search_time_increase(pretty_response(response))
    return pretty_response(response)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=31001)