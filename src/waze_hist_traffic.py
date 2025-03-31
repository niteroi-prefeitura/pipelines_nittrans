from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret

from tasks.get_api_data import get_api_data_as_json
from tasks.parse_to_dataframe import parse_traffic_hist_data
from utils.portal_layer_methods import get_layer_on_portal, generate_portal_token, create_new_feature

secret_block = Secret.load("usuario-integrador-agol")
user_agol = secret_block.get()

secret_block = Secret.load("usuario-pmngeo-portal")
user_portal = secret_block.get()

his_layers_url = Variable.get("waze_hist_portal_layers")
URL_WAZE_API = Variable.get("url_waze_api")["URL"]

CREDENTIALS_AGOL = {
    "agol_username": user_agol["username"],
    "agol_password": user_agol["password"],   
}

CREDENTIALS_PORTAL = {
    "username": user_portal["username"],
    "password": user_portal["password"],   
}

URL_TRAFFIC_HIST_PORTAL = his_layers_url["URL_TRAFEGO"]

@flow(name="waze-traffic-hist",log_prints=True)
def waze_traffic_hist():
    try:
        waze_data = get_api_data_as_json(URL_WAZE_API)

        parsed_data = parse_traffic_hist_data(waze_data)

        filtered_df = parsed_data[parsed_data["li_nivel"] >= 4]

        if not filtered_df.empty:

            hist_feats = []

            for _, row in filtered_df.iterrows():
                att = row.to_dict()

                hist_feats.append({
                    "attributes": att
                })
            
            token = generate_portal_token(CREDENTIALS_PORTAL)

            portal_layer = get_layer_on_portal(URL_TRAFFIC_HIST_PORTAL, token) 

            create_new_feature(hist_feats,portal_layer)
        else:
            print('Sem registros a serem adicionados')
        
       
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    waze_traffic_hist()
