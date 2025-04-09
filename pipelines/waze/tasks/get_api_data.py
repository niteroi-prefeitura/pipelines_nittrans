import requests
from prefect import task, get_run_logger

@task(name="Buscar dados API")
def get_api_data_as_json(url):
    logger = get_run_logger()

    try:                
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, dict):
            raise ValueError(f"Erro ao obter dados da API em JSON: {data}")
        
        logger.info("Dados obtidos com sucesso")     
        
        return data
    
    except requests.exceptions.RequestException as e:
        raise ValueError("Erro ao acessar a API: {e}")
