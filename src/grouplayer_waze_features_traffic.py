from dotenv import load_dotenv
import os
import requests
import pandas as pd
from registers import clear_old_registers, add_registers
from send_email import send_email_error
from datetime import datetime
from arcgis.gis import GIS
from smtplib import SMTP

load_dotenv()

AGOL_USERNAME = os.getenv("AGOL_USERNAME")
AGOL_PASSWORD = os.getenv("AGOL_PASSWORD")
LAYER_ID = os.getenv("LAYER_ID")

def get_api_data(url):

    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def process_data(resposta_json):

    df = pd.DataFrame(resposta_json['jams'])
    if not df.empty:
        df['Pais'] = "BR"
        df['Cidade'] = "NITEROI"
        df['Vel_km_h'] = df['speedKMH']
        df['Tipo_da_rua'] = df['roadType']

        now = datetime.now()
        df['Data'] = now.strftime(format='%d/%m/%Y %H:%M')
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True)
        df['Data'] = df['Data'].apply(lambda x: int(x.timestamp() * 1000))

    return df

def main():
    log_erros = []
    
    url = "https://www.waze.com/row-partnerhub-api/partners/18863635230/waze-feeds/feef83ff-1b4f-4d93-9d7a-dd3ea11304d3?format=1"
    
    json_response = get_api_data(url)
    
    if isinstance(json_response, str):
        log_erros.append(json_response)
        send_email_error(log_erros)
        return
    
    df = process_data(json_response)
    if isinstance(df, str):
        log_erros.append(df)
        send_email_error(log_erros)
        return
    
    try:
        gis = GIS("https://www.arcgis.com", AGOL_USERNAME, AGOL_PASSWORD)
        layer_id = LAYER_ID
        portal_item = gis.content.get(layer_id)
        traffic_agol = portal_item.layers[2]
        
    except Exception as e:
        log_erros.append(f"Erro ao conectar no ArcGIS: {e}")
        send_email_error(log_erros)
        return
    
    registers_removed = clear_old_registers(traffic_agol)
    if isinstance(registers_removed, str):
        log_erros.append(registers_removed)
    
    registers_added = add_registers(df, traffic_agol)
    if isinstance(registers_added, str):
        log_erros.append(registers_added)
    
    send_email_error(log_erros)

if __name__ == "__main__":
    main()