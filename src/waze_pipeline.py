from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret

from tasks.get_api_data import get_api_data_as_json
from tasks.parse_to_dataframe import parse_api_data
from utils.agol_layer_methods import get_layer_agol, query_layer_agol
from sub.live_alerts import waze_live_alerts
from sub.live_traffic import waze_live_traffic

secret_block = Secret.load("usuario-integrador-agol")
user_agol = secret_block.get()

URL_WAZE_API = Variable.get("url_waze_api")["URL"]
LIVE_ALERTS_LAYER_ID_AGOL = Variable.get("waze_live_layer_id_agol")["ALERTS"]
LIVE_TRAFFIC_LAYER_ID_AGOL = Variable.get("waze_live_layer_id_agol")["TRAFFIC"]

CREDENTIALS_AGOL = {
    "agol_username": user_agol["username"],
    "agol_password": user_agol["password"],   
}

@flow(name="waze-live-hist",log_prints=True)
def waze_live():
    try:
        #Busca Dados na API
        waze_data = get_api_data_as_json(URL_WAZE_API)


        #Formata os dados para um dataframe compatível com a camada
        dfs_api = parse_api_data(waze_data)


        #Busca a camada de alertas no agol
        live_alerts_layer = get_layer_agol(
                CREDENTIALS_AGOL,LIVE_ALERTS_LAYER_ID_AGOL,0)        
        live_traffic_layer = get_layer_agol(CREDENTIALS_AGOL, LIVE_TRAFFIC_LAYER_ID_AGOL, 0)
        
        
        #Cria dataframe da camada    
        df_live_alerts_layer = query_layer_agol(
            live_alerts_layer)        
        df_live_traffic_layer = query_layer_agol(
            live_traffic_layer)
        
        if not dfs_api["alerts"].empty:
            waze_live_alerts(dfs_api["alerts"], df_live_alerts_layer, live_alerts_layer)

        if not dfs_api["traffic"].empty:      
            waze_live_traffic(dfs_api["traffic"], df_live_traffic_layer, live_traffic_layer)
        
       
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    waze_live()
