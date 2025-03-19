import os
from utils.get_api_data import get_api_data_as_json
from utils.parse_dataframe import parse_api_data
from utils.agol_layer_methods import get_layer_agol, query_layer_agol
from utils.compare_attributes import compare_attributes
from utils.tasks import task_only_in_api, task_only_in_layer, task_matching_att
import pandas as pd
from dotenv import load_dotenv
from prefect import flow,get_run_logger
from prefect.variables import Variable
from prefect.blocks.system import Secret

secret_block = Secret.load("usuario-integrador-agol")
secret_block = Secret.load("usuario-pmngeo-portal")
user_agol = secret_block.get()
user_portal = secret_block.get()
load_dotenv()

URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL") or Variable.get("url_waze_api")["URL"]
LIVE_LAYER_ID_AGOL = os.getenv("LAYER_WAZE_AGOL") or Variable.get("waze_live_layer_id_agol")["ID_TESTE"]

CREDENTIALS_AGOL = {
    "agol_username": os.getenv("AGOL_USERNAME") or user_agol["username"],
    "agol_password": os.getenv("AGOL_PASSWORD") or user_agol["password"],   
}

@flow(name="waze-live-hist",log_prints=True)
def waze_live_hist():
    try:
        logger = get_run_logger()
        logger.info("Iniciando o fluxo")
        #Busca Dados na API
        waze_data = get_api_data_as_json(URL_WAZE_API)

        #Formata os dados para um dataframe compatível com a camada
        df_api = parse_api_data(waze_data)

        #Busca a camada de alertas no agol
        live_layer = get_layer_agol(
                CREDENTIALS_AGOL,LIVE_LAYER_ID_AGOL, 0)
        
        #Cria dataframe da camada    
        df_live_layer = query_layer_agol(
            live_layer)
        
        #Compara "tx_uuid"'s vindos da API com os "uuid" que já estão na camada live
        compared_data = compare_attributes(
            df_api, "uuid", df_live_layer, "uuid")
        
        #Cria uma "Serie" com apenas os itens que estão presentes no dataframe da API
        only_in_API = df_api[df_api['uuid'].isin(
            compared_data["only_in_df"])] if len(compared_data["only_in_df"]) > 0 else pd.DataFrame([])
        
        #Cria uma "Serie" com os itens que estão presentes simultaneamente no dataframe da API e na camada live
        matching_attributes = df_api[df_api['uuid'].isin(
            compared_data["matching_attributes"])] if len(compared_data["matching_attributes"]) > 0 else pd.DataFrame([])
        
        #Cria uma "Serie" com os itens que estão presentes apenas na camada live
        only_in_layer = df_live_layer[df_live_layer['uuid'].isin(
            compared_data["only_in_layer"])] if len(compared_data["only_in_layer"]) > 0 else pd.DataFrame([])

        #Quando os itens comparados estiverem presentes apenas no dataframe da API devem ser incluídos na camada live
        # com preenchendo a data de criação no campo startTime        
        if only_in_API is not None and not only_in_API.empty: 
            task_only_in_api(only_in_API, live_layer)
        
        #Quando os itens comparados estiverem presentes apenas na camada live devem ser excluídos da camada live
        # e incluídos na camada de histórico preenchendo o valor dt_saída com a data referente a exclusão
        if only_in_layer is not None and not only_in_layer.empty:   
            task_only_in_layer(only_in_layer, live_layer)            

        #Quando os itens comparados estiverem presentes em ambos os dataframes o valor do campo "Atualizado" deve ser preenchido com a data atual
        if matching_attributes is not None and not matching_attributes.empty: 
            task_matching_att(df_live_layer,compared_data, matching_attributes, live_layer)
       
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    waze_live_hist()