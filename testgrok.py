import os
import requests
from config import load_proxy_env, get_proxy_dict

load_proxy_env()
PROXIES = get_proxy_dict()

API_URL = "https://api.x.ai/v1/chat/completions"

payload = {
    "messages": [
        {"role": "user", "content": "告诉我今天crypto领域发生的3件大事"}
    ],
    "search_parameters": {
        "mode": "auto",
        "return_citations": True,
    },
    "model": "grok-3-latest",
}

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {os.getenv('XAI_API_KEY')}"
}

response = requests.post(
    API_URL, headers=headers, json=payload, proxies=PROXIES or None
)
print(response.json())
