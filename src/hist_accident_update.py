import os
from dotenv import load_dotenv
from utils.generate_token import generate_portal_token
from utils.get_layer_on_arcgis import get_a_layer_object_agol, get_atributes_of_layer_object
from utils.process_data import process_data_waze_alerts
from utils.get_api_data import get_api_data_as_json
from utils.compare_attributes import compare_attributes
from utils.filter_waze_alerts import filter_waze_alerts_by_alert_type
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

credentials_to_get_layer_on_agol = {
    "agol_username": os.getenv("AGOL_USERNAME"),
    "agol_password": os.getenv("AGOL_PASSWORD"),
    "layer_id": os.getenv("LAYER_ID"),
}

credentials_to_send_data_to_portal = {
    "layer_name": "NITTRANS_WAZE_P_HIST_ACIDENTE",
    "item": 0
}

URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL")


def main():
    try:
        waze_data = get_api_data_as_json(URL_WAZE_API)

        waze_alerts = process_data_waze_alerts(waze_data)

        df_waze_accidents = filter_waze_alerts_by_alert_type(
            waze_alerts, 'ACCIDENT')

        if df_waze_accidents is not None and not df_waze_accidents.empty:
            print(f'{len(df_waze_accidents)} Acidentes encontrados na API do Waze.')

            alerts_layer = get_a_layer_object_agol(
                credentials_to_get_layer_on_agol, 1)

            alerts_layer_with_uuid_and_tipe = get_atributes_of_layer_object(
                alerts_layer, "uuid, Tipo")

            compared_data = compare_attributes(
                df_waze_accidents, "tx_uuid", alerts_layer_with_uuid_and_tipe, "uuid")

            accidents_only_in_df = df_waze_accidents[df_waze_accidents['tx_uuid'].isin(
                compared_data["only_in_df"])]

            if accidents_only_in_df is not None and not accidents_only_in_df.empty:
                token = generate_portal_token(credentials_to_generate_token)

                update_point_layers_on_portal(
                    accidents_only_in_df, credentials_to_send_data_to_portal, token)
            else:
                print("Sem novos acidentes para adicionar ao histórico.")
                return
        else:
            print("Nenhum acidente encontrado na API do Waze.")
            return
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    main()
