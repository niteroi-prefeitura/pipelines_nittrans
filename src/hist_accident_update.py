import os
import pandas as pd
from dotenv import load_dotenv
from utils.send_email import send_email_error
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

teste_credentials_to_send_data_to_portal = {
    "layer_name": "TESTE_P_HIST_ACIDENTE",
    "item": 0
}

URL_WAZE_API = os.getenv("WAZE_PARTNER_HUB_API_URL")


def main():
    try:
        data = get_api_data_as_json(URL_WAZE_API)

        # Trazer dataframe tratado sem filtro baseado na API do Waze
        df_alerts = process_data_waze_alerts(data)

        df_accidents = filter_waze_alerts_by_alert_type(df_alerts, 'ACCIDENT')

        # Buscar layer de alertas no agol
        alerts_layer = get_a_layer_object_agol(
            credentials_to_get_layer_on_agol, 1)

        # Criar função para pegar os atributos da alerts_layer
        alerts_layer_attributes = get_atributes_of_layer_object(
            alerts_layer, "uuid, Tipo")

        # comparar os atributos do alerts_layer com os atributos do df_tratado
        attributes_comparison = compare_attributes(
            df_accidents, "tx_uuid", alerts_layer_attributes, "uuid")

        print("Atributos iguais: ",
              attributes_comparison["matching_attributes"])
        print("Atributos apenas no df: ", attributes_comparison["only_in_df"])
        print("Atributos apenas na layer: ",
              attributes_comparison["only_in_layer"])

        accidents_only_in_df = df_accidents[df_accidents['tx_uuid'].isin(
            attributes_comparison["only_in_df"])]

        matching_accidents = df_accidents[df_accidents['tx_uuid'].isin(
            attributes_comparison["matching_attributes"])]

        alerts_layer_attributes = pd.DataFrame(alerts_layer_attributes)

        accidents_only_in_layer = alerts_layer_attributes[
            (alerts_layer_attributes['uuid'].isin(
                attributes_comparison["only_in_layer"]))
            & (alerts_layer_attributes['Tipo'] == 'Trânsito parado')
        ]

        # accidents_only_in_layer = alerts_layer_attributes[alerts_layer_attributes['uuid'].isin(
        #     attributes_comparison["only_in_layer"])]

        print("Acidentes no DataFrame e na Layer: ", matching_accidents)
        print("Acidentes apenas no DataFrame : ", accidents_only_in_df)
        print("Acidentes apenas na Layer: ", accidents_only_in_layer)

        # Se os atributos forem iguais, atualizar apneas o campo de data na layer

        # print("Tentando gerar token...")
        # token = generate_portal_token(credentials_to_generate_token)
        # print("Token gerado com sucesso! Valor de token: ", token)
        # print("Valor de alerts_layer: ", alerts_layer)
        # update_point_layers_on_portal(
        #     df_accident, teste_credentials_to_send_data_to_portal, token, alerts_layer)

    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução: {error_message}")
        send_email_error(credentials_to_send_error_email,
                         'Erro no Script de Histórico de Acidentes Waze', error_message)


if __name__ == "__main__":
    main()
