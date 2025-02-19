import requests


def get_api_data_as_json(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Erro ao obter dados da API em JSON: {data}")
        return data
    except requests.exceptions.RequestException as e:
        return f"Erro ao acessar a API: {e}"
