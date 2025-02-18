import os
from dotenv import load_dotenv
from utils.send_email import send_email_error
from utils.generate_token import generate_portal_token
from utils.get_layer_on_arcgis import get_a_layer_index

load_dotenv()

credentials_to_generate_token = {
    "portal_username": os.getenv("PORTAL_USERNAME"),
    "portal_password": os.getenv("PORTAL_PASSWORD"),
    "portal_referer": os.getenv("PORTAL_REFERER"),
    "url_to_generate_token": os.getenv("URL_TO_GENERATE_TOKEN")
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


def main():
    try:
        token = generate_portal_token(credentials_to_generate_token)
        alerts_layer = get_a_layer_index(credentials_to_get_layer_on_arcgis, 1)
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução: {error_message}")
        send_email_error(credentials_to_send_error_email,
                         'Erro no Script de Histórico de Acidentes Waze', error_message)
