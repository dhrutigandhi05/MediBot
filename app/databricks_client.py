import os
import requests
from dotenv import load_dotenv

load_dotenv()

class DatabricksClient:
    def __init__(self):
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        classifier = os.getenv("CLASSIFIER_ENDPOINT")
        retriever = os.getenv("RETRIEVER_ENDPOINT")

        if not host:
            raise RuntimeError("Missing DATABRICKS_HOST")
        if not token:
            raise RuntimeError("Missing DATABRICKS_TOKEN")
        if not classifier:
            raise RuntimeError("Missing CLASSIFIER_ENDPOINT")
        if not retriever:
            raise RuntimeError("Missing RETRIEVER_ENDPOINT")

        self.host = host.rstrip("/")  # remove trailing /
        self.token = token
        self.classifier = classifier
        self.retriever = retriever
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _invoke(self, endpoint: str, record: dict) -> dict:
        url = f"{self.host}/serving-endpoints/{endpoint}/invocations"
        payload = {"dataframe_records": [record]}  # 1-row dataframe
        response = requests.post(url, headers=self.headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()

    def classify(self, question: str, threshold: float = 0.80) -> dict:
        return self._invoke(self.classifier, {"question": question, "threshold": float(threshold)})

    def retrieve(self, question: str, top_k: int = 5, pool_k: int = 50, max_dist: float = 0.85) -> dict:
        return self._invoke(
            self.retriever,
            {
                "question": question,
                "top_k": int(top_k),
                "pool_k": int(pool_k),
                "max_dist": float(max_dist),
            },
        )