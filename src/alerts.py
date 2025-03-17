import os
from utils.get_api_data import get_api_data_as_json
from utils.process_data import parse_api_data, create_ms_timestamp, parse_hist_data
from utils.agol_layers import get_layer_agol, query_layer_agol, update_features_agol, add_features_agol, remove_from_agol
from utils.compare_attributes import compare_attributes
from utils.filter_waze_alerts import filter_by_alert_type
from utils.update_layers_on_portal import build_new_hist_feature, get_layer_on_portal, create_new_feature
import pandas as pd
import json
from dotenv import load_dotenv
""" from prefect.variables import Variable
from prefect.blocks.system import Secret """

""" secret_block = Secret.load("usuario-integrador-agol")
user_agol = secret_block.get() """
load_dotenv()

""" URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL") or Variable.get("url_waze_api")["URL"]
LAYER_ID_AGOL = os.getenv("LAYER_ID_AGOL") or Variable.get("waze_live_layer_id_agol")["ID"],
CREDENTIALS_AGOL = {
    "agol_username": os.getenv("AGOL_USERNAME") or user_agol["username"],
    "agol_password": os.getenv("AGOL_PASSWORD") or user_agol["password"],   
} """

URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL")
LAYER_ID_AGOL = os.getenv("TESTE_LAYER_AGOL")
CREDENTIALS_AGOL = {
    "agol_username": os.getenv("AGOL_USERNAME"),
    "agol_password": os.getenv("AGOL_PASSWORD"),   
}
URL_HIST_LAYER_PORTAL = os.getenv("URL_HIST_LAYER_PORTAL")

def main():
    try:
        #Busca Dados na API
        waze_data = get_api_data_as_json(URL_WAZE_API)

        #Formata os dados para um dataframe compatível com a camada
        df_api = parse_api_data(waze_data)

        #Busca a camada de alertas no agol
        live_layer = get_layer_agol(
                CREDENTIALS_AGOL,LAYER_ID_AGOL, 0)
        
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
            create_ms_timestamp(only_in_API,'startTime')
            features_to_add = []
            for _, row in only_in_API.iterrows():
                new_feature = {
                    "attributes": row.to_dict(),
                    "geometry": {
                    "x": float(row['Lng']),
                    "y": float(row['Lat']),
                    "spatialReference": {"wkid": 4674} 
                    }
                }
                features_to_add.append(new_feature)

            add_features_agol(live_layer, features_to_add)  
        
        #Quando os itens comparados estiverem presentes apenas na camada live devem ser excluídos da camada live
        # e incluídos na camada de histórico preenchendo o valor dt_saída com a data referente a exclusão
        if only_in_layer is not None and not only_in_layer.empty:   
            create_ms_timestamp(only_in_layer, 'endTime')       
            parsed_data = parse_hist_data(only_in_layer)

            acidentes = pd.DataFrame(parsed_data[parsed_data['tx_tipo_alerta'] == 'Acidente'])

            if len(acidentes) > 0:
                feats = build_new_hist_feature(acidentes)
                portal_layer = get_layer_on_portal(URL_HIST_LAYER_PORTAL)            
                result = create_new_feature(feats,portal_layer)
                if result == True:
                    remove_from_agol(live_layer,only_in_layer)  
            

        #Quando os itens comparados estiverem presentes em ambos os dataframes o valor do campo "Atualizado" deve ser preenchido com a data atual
        if matching_attributes is not None and not matching_attributes.empty: 
            live_matching = df_live_layer[df_live_layer['uuid'].isin(
            compared_data["matching_attributes"])] 
            features_to_update = []
            
            for _, row_api in matching_attributes.iterrows():
                for _, row_live in live_matching.iterrows():
                    if row_live['uuid'] == row_api['uuid']:
                        feature = {
                            "attributes": {
                                **row_api.to_dict(),
                                "OBJECTID": row_live["OBJECTID"]
                                },
                        }
                        features_to_update.append(feature)            

            update_features_agol(live_layer,features_to_update)      
       
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    main()