import os
from dotenv import load_dotenv
from utils.send_email import send_email_error
from utils.generate_token import generate_portal_token
from utils.get_layer_on_arcgis import get_a_layer_index
from utils.process_data import process_data_hist_accident
from utils.get_api_data import get_api_data_as_json
from utils.update_layers_on_portal import update_point_layers_on_portal

load_dotenv()

credentials_to_generate_token = {
    "portal_username": os.getenv("PORTAL_USERNAME"),
    "portal_password": os.getenv("PORTAL_PASSWORD"),
    "portal_referer": os.getenv("URL_GIS_ENTERPRISE")
}

credentials_to_send_error_email = {
    "sender_email_address": os.getenv('SENDER_EMAIL_ADDRESS'),
    "sender_email_password": os.getenv('SENDER_EMAIL_PASSWORD'),
    "recipient_email_address": os.getenv('RECIPIENT_EMAIL_ADDRESS'),
}

credentials_to_get_layer_on_arcgis = {
    "agol_username": os.getenv("AGOL_USERNAME"),
    "agol_password": os.getenv("AGOL_PASSWORD"),
    "layer_id": os.getenv("LAYER_ID"),
}

credentials_to_send_data_to_portal = {
    "url_gis_enterprise": os.getenv("URL_GIS_ENTERPRISE"),
    "layer_name": "NITTRANS_WAZE_P_HIST_ACIDENTE",
    "item": 0
}

URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL")


def main():
    try:
        data = get_api_data_as_json(URL_WAZE_API)
        df_accident = process_data_hist_accident(data)
        if df_accident is None:

            return
        print("Tentando gerar token...")
        token = generate_portal_token(credentials_to_generate_token)
        print("Token gerado com sucesso! Valor de token: ", token)
        alerts_layer = get_a_layer_index(credentials_to_get_layer_on_arcgis, 1)
        update_point_layers_on_portal(
            df_accident, credentials_to_send_data_to_portal, token, alerts_layer)

    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução: {error_message}")
        send_email_error(credentials_to_send_error_email,
                         'Erro no Script de Histórico de Acidentes Waze', error_message)


if __name__ == "__main__":
    main()
