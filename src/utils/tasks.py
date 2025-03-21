from utils.agol_layer_methods import add_features_agol, remove_from_agol, update_features_agol
from utils.parse_dataframe import create_ms_timestamp, parse_hist_data
from utils.portal_layer_methods import build_new_hist_feature, get_layer_on_portal, create_new_feature, generate_portal_token
from prefect.variables import Variable
from prefect import flow, get_run_logger
from prefect.blocks.system import Secret
import pandas as pd

secret_block = Secret.load("usuario-pmngeo-portal")
user_portal = secret_block.get()
his_layers_url = Variable.get("waze_hist_portal_layers")

CREDENTIALS_PORTAL = {
    "username": user_portal["username"],
    "password": user_portal["password"],   
}

URL_ACCIDENT_HIST_PORTAL = his_layers_url["URL_ACIDENTES"]
URL_POT_HOLE_HIST_PORTAL = his_layers_url["URL_BURACOS"]
URL_CAR_STOPPED_HIST_PORTAL = his_layers_url["URL_CARRO_PARADO"]
URL_TRAFFIC_LIGHT_FAULT_HIST_PORTAL = his_layers_url["URL_FALHA_SEMAFORO"]
URL_CONSTRUCTION_HIST_PORTAL = his_layers_url["URL_OBRA"]
URL_LANE_CLOSED_HIST_PORTAL = his_layers_url["URL_VIA_FECHADA"]
URL_POLICE_HIST_PORTAL = his_layers_url["URL_POLICIA"]
URL_FLOOD_HIST_PORTAL = his_layers_url["URL_ALAGAMENTO"]
URL_HAZARD_ROAD_HIST_PORTAL = his_layers_url["URL_PERIGO_PISTA"]
URL_HAZARD_OBJECT_HIST_PORTAL = his_layers_url["URL_OBJETO_PISTA"]


@flow(name="Fluxo dados apenas na api", description="Insere data de entrada, cria features georreferenciadas e adiciona a camada live")
def sub_only_in_api(df_api,live_layer):
    logger = get_run_logger()
    
    logger.info('Inicia fluxo para dados apenas na api')
    
    logger.info('only_in_api: Insere data de entrada, cria features georreferenciadas e adiciona a camada live')
    
    try:        
        create_ms_timestamp(df_api,'startTime')
        features_to_add = []
        for _, row in df_api.iterrows():
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
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução only_in_api: {error_message}")

@flow(name="Fluxo dados apenas na live", description="Insere data de saída, trata dados para formato de histórico, filtra por contexto, insere as features na hist e excluí da live")
def sub_only_in_layer(df_layer, live_layer):
    logger = get_run_logger()

    logger.info('Inicia fluxo para dados apenas na live')

    logger.info('only_in_layer: Insere data de saída, trata dados para formato de histórico, filtra por contexto, insere as features na hist e excluí da live')
    
    try:
        create_ms_timestamp(df_layer, 'endTime')       
        parsed_data = parse_hist_data(df_layer)
        token = generate_portal_token(CREDENTIALS_PORTAL)

        df_map = {
            "acidentes": pd.DataFrame(parsed_data[parsed_data['tx_tipo_alerta'] == 'Acidente']),
            "buracos": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Buraco']),
            "semaforo": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Falha no semáforo']),
            "carro_parado": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'].isin(['Carro parado na pista','Carro parado no acostamento'])]),
            "obras": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Obra na pista']),
            "via_fechada": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Uma via fechada']),
            "policia": pd.DataFrame(parsed_data[parsed_data['tx_tipo_alerta'] == 'Polícia']),
            "alagamento": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Alagamento']),
            "perigo_pista": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Perigo na pista']),
            "objeto_pista": pd.DataFrame(parsed_data[parsed_data['tx_subtipo_alerta'] == 'Objeto na pista'])
        }

        url_map = {
            "acidentes": URL_ACCIDENT_HIST_PORTAL,
            "buracos": URL_POT_HOLE_HIST_PORTAL,
            "semaforo": URL_TRAFFIC_LIGHT_FAULT_HIST_PORTAL,
            "carro_parado": URL_CAR_STOPPED_HIST_PORTAL,
            "obras": URL_CONSTRUCTION_HIST_PORTAL,
            "via_fechada": URL_LANE_CLOSED_HIST_PORTAL,
            "policia": URL_POLICE_HIST_PORTAL,
            "alagamento": URL_FLOOD_HIST_PORTAL,
            "perigo_pista": URL_HAZARD_ROAD_HIST_PORTAL,
            "objeto_pista": URL_HAZARD_OBJECT_HIST_PORTAL
        }

        for df_nome, df in df_map.items():
            PORTAL_URL = None
            for url_nome, url in url_map.items():
                if url_nome == df_nome:
                    PORTAL_URL = url

            if len(df) > 0:
                logger.info(f'{df_nome}: {len(df)}')
                feats = build_new_hist_feature(df)
                portal_layer = get_layer_on_portal(PORTAL_URL, token)            
                result = create_new_feature(feats,portal_layer)
                if result == True:
                    remove_from_agol(live_layer,df)
                else:
                    logger.info("Os registros antigos não foram removidos")    
          

    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução only_in_layer: {error_message}")

@flow(name="Fluxo dados em ambos", description="Seleciona no df_live as features que estão na live e na api , cria features com valores novos, atualiza features na camada live")
def sub_matching_att(df_live_layer,compared_data, matching_attributes, live_layer):
    logger = get_run_logger()
    
    logger.info('Inicia fluxo para dados que estão na live e na api')
    
    logger.info('only_in_layer: Seleciona no df_live as features que estão na live e na api , cria features com valores novos, atualiza features na camada live')
    
    try:
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
        print(f"Erro durante a execução matching_attributes: {error_message}")