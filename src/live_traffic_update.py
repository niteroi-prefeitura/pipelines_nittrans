import os
from dotenv import load_dotenv
from utils.process_data import process_data_live_traffic
from utils.send_email import send_email_error
from utils.get_api_data import get_api_data_as_json
from utils.get_layer_on_arcgis import get_a_layer_index
from utils.replace_layers_on_arcgis import replace_polyline_layers_on_arcgis

load_dotenv()

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

attribute_map = {
    'Pais': 'Pais',
    'uuid': 'uuid',
    'street': 'street',
    'Final': 'endNode',
    'Tipo_da_rua': 'Tipo_da_rua',
    'Comprimento': 'length',
    'Vel_km_h': 'Vel_km_h',
    'Cidade': 'Cidade',
    'Level_': 'level',
    'delay': 'delay',
    'speed': 'speed',
    'turnType': 'turnType',
    'Data': 'Data'
}

WAZE_PARTNER_HUB_API_URL = os.getenv("WAZE_PARTNER_HUB_API_URL")


def main():
    errors = []

    try:
        json_response = get_api_data_as_json(WAZE_PARTNER_HUB_API_URL)

        df = process_data_live_traffic(json_response)

        traffic_layer = get_a_layer_index(
            credentials_to_get_layer_on_arcgis, 2)

        errors += replace_polyline_layers_on_arcgis(
            traffic_layer, df, attribute_map)

    except Exception as e:
        errors.append(str(e))

    finally:
        if errors:
            send_email_error(credentials_to_send_error_email,
                             'Erro no Script de Live Traffic do Waze', errors)


if __name__ == "__main__":
    main()
