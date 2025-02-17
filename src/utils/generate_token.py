import requests
from typing import Dict


def generate_portal_token(credentials: Dict[str, str]):

    params = {
        "username": credentials["portal_username"],
        "password": credentials["portal_password"],
        "referer": credentials["portal_referer"],
        "f": "json",
    }

    response = requests.post(credentials["url_to_generate_token"], data=params)

    if response.status_code == 200:
        token = response.json().get("token")
        print(f"Token gerado: {token}")
        return token
    else:
        print(f"Erro ao gerar token: {response.text}")
