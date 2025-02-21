import os
import requests
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

URL_TO_GENERATE_TOKEN = os.getenv("URL_TO_GENERATE_TOKEN")


def generate_portal_token(credentials: Dict[str, str]):

    params = {
        "username": credentials["portal_username"],
        "password": credentials["portal_password"],
        "referer": credentials["portal_referer"],
        "f": "json",
    }

    response = requests.post(URL_TO_GENERATE_TOKEN, data=params)

    if response.status_code == 200:
        token_info = response.json()
        return token_info['token']
    else:
        print(f"Erro ao gerar token: {response.text}")
