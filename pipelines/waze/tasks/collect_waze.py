from prefect import task
import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os


@task(name="Buscar dados", description="Busca dados da API Waze e salva arquivo JSON")
def get_waze_partnerhub_json(api_url, file_path):
    """
    Baixa dados da API Waze e salva como arquivo raw JSON.
    O nome do arquivo é construído a partir do campo endTimeMillis do JSON retornado.
    A estrutura de pastas segue a data de endTimeMillis (YYYY/MM/DD/HH).
    Se o arquivo já existir, ele será sobrescrito.

    Args:
        api_url (str): URL da API do Waze
        file_path (str): Caminho base do diretório para salvar

    Returns:
        str: Caminho completo do arquivo salvo ou None se erro
    """
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            # Pega a data da raiz do JSON
            endTimeMillis = data.get("endTimeMillis")

            if endTimeMillis is None:
                print("JSON não possui campo endTimeMillis")
                return None

            # Converte para datetime (Spark format: yyyy-MM-dd HH:mm:ss)
            end_dt = datetime.utcfromtimestamp(endTimeMillis // 1000)

            # Monta o nome do arquivo no formato solicitado
            # filename = f"waze_partnerhub_YYYYMMDD_HHMMSS.json"
            filename = f"waze_partnerhub_{end_dt.strftime('%Y%m%d_%H%M%S')}.json"

            # Monta pasta baseada em endTimeMillis: {file_path}/YYYY/MM/DD/HH/
            subdir = os.path.join(
                file_path,
                end_dt.strftime("%Y"),
                end_dt.strftime("%m"),
                end_dt.strftime("%d"),
            )
            os.makedirs(subdir, exist_ok=True)

            # Caminho completo do arquivo
            full_path = os.path.join(subdir, filename)

            # Salva arquivo JSON (sobrescreve se existir)
            with open(full_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"Dados salvos em: {full_path}")
            return data
        else:
            print(f"Erro ao acessar API: {response.status_code}")
            return None
    except Exception as e:
        print(f"Erro durante download: {str(e)}")
        return None
