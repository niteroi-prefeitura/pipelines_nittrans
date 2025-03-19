from utils.agol_layer_methods import add_features_agol, remove_from_agol, update_features_agol
from utils.parse_dataframe import create_ms_timestamp, parse_hist_data
from utils.portal_layer_methods import build_new_hist_feature, get_layer_on_portal, create_new_feature, generate_portal_token
from prefect.variables import Variable
from prefect import flow
from prefect.blocks.system import Secret
import pandas as pd

secret_block = Secret.load("usuario-pmngeo-portal")
user_portal = secret_block.get()

CREDENTIALS_PORTAL = {
    "username": user_portal["username"],
    "password": user_portal["password"],   
}

URL_ACCIDENT_HIST_PORTAL = Variable.get("url_accident_hist_portal")["URL"]

@flow(name="Fluxo dados apenas na api", description="Insere data de entrada, cria features georreferenciadas e adiciona a camada live")
def sub_only_in_api(df_api,live_layer):
    print('Inicia fluxo para dados apenas na api')
    print('only_in_api: Insere data de entrada, cria features georreferenciadas e adiciona a camada live')
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
    print('Inicia fluxo para dados apenas na live')
    print('only_in_layer: Insere data de saída, trata dados para formato de histórico, filtra por contexto, insere as features na hist e excluí da live')
    try:
        create_ms_timestamp(df_layer, 'endTime')       
        parsed_data = parse_hist_data(df_layer)

        acidentes = pd.DataFrame(parsed_data[parsed_data['tx_tipo_alerta'] == 'Acidente'])

        if len(acidentes) > 0:
            print(f'acidentes: {len(acidentes)}')
            feats = build_new_hist_feature(acidentes)
            token = generate_portal_token(CREDENTIALS_PORTAL)
            portal_layer = get_layer_on_portal(URL_ACCIDENT_HIST_PORTAL, token)            
            result = create_new_feature(feats,portal_layer)
            if result == True:
                remove_from_agol(live_layer,df_layer) 

    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução only_in_layer: {error_message}")

@flow(name="Fluxo dados em ambos", description="Seleciona no df_live as features que estão na live e na api , cria features com valores novos, atualiza features na camada live")
def sub_matching_att(df_live_layer,compared_data, matching_attributes, live_layer):
    print('Inicia fluxo para dados que estão na live e na api')
    print('only_in_layer: Seleciona no df_live as features que estão na live e na api , cria features com valores novos, atualiza features na camada live')
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