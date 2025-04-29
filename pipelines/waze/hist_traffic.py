from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret
from dotenv import load_dotenv
from tasks.get_api_data import get_api_data_as_json
from tasks.parse_to_dataframe import parse_traffic_hist_data
from utils.portal_layer_methods import get_layer_on_portal, generate_portal_token, create_new_feature
import os

load_dotenv()
gis_variables = Variable.get("gis_portal_variables")
his_layers_url = Variable.get("waze_hist_portal_layers")
user_agol = Secret.load("usuario-integrador-agol").get()
user_portal = Secret.load("usuario-pmngeo-portal").get()

URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL") or Secret.load(
    "waze_url_autenticada_api")
URL_TRAFFIC_HIST_PORTAL = os.getenv(
    "URL_TRAFFIC_HIST_PORTAL") or his_layers_url["URL_TRAFEGO"]
URL_TO_GENERATE_TOKEN = os.getenv(
    "URL_TO_GENERATE_TOKEN") or gis_variables["URL_TO_GENERATE_TOKEN"]
URL_GIS_ENTERPRISE = os.getenv(
    "URL_GIS_ENTERPRISE") or gis_variables["URL_GIS_ENTERPRISE"]

CREDENTIALS_AGOL = {
    "agol_username": os.getenv("AGOL_USERNAME") or user_agol["username"],
    "agol_password": os.getenv("AGOL_PASSWORD") or user_agol["password"],
}

CREDENTIALS_PORTAL = {
    "username": os.getenv("PORTAL_USERNAME") or user_portal["username"],
    "password": os.getenv("PORTAL_PASSWORD") or user_portal["password"],
}


@flow(name="Histórico de Tráfego", log_prints=True)
def waze_traffic_hist():
    try:
        waze_data = get_api_data_as_json(URL_WAZE_API)

        parsed_data = parse_traffic_hist_data(waze_data)

        filtered_df = parsed_data[parsed_data["li_nivel"]
                                  >= 4] if not parsed_data.empty else parsed_data

        if not filtered_df.empty:

            hist_feats = []

            for _, row in filtered_df.iterrows():
                att = row.to_dict()

                hist_feats.append({
                    "attributes": att
                })

            token = generate_portal_token(
                CREDENTIALS_PORTAL, URL_GIS_ENTERPRISE, URL_TO_GENERATE_TOKEN)

            portal_layer = get_layer_on_portal(
                URL_TRAFFIC_HIST_PORTAL, token, URL_GIS_ENTERPRISE)

            create_new_feature(hist_feats, portal_layer)
        else:
            print('Sem registros a serem adicionados')

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    waze_traffic_hist()
