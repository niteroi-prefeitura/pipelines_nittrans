from dotenv import load_dotenv
import os
import requests
import pandas as pd
from registros import limpar_registros_antigos, adicionar_registros
from datetime import datetime, timedelta
from arcgis.gis import GIS
from smtplib import SMTP
from email.mime.text import MIMEText

load_dotenv()

EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
EMAIL_SENHA = os.getenv("EMAIL_SENHA")
EMAIL_DESTINATARIO = os.getenv("EMAIL_DESTINATARIO")
LAYER_ID = os.getenv("LAYER_ID")

# Função para obter dados da API
def obter_dados_api(url):

    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Função para processar os dados e criar o DataFrame
def processar_dados(resposta_json):

    df = pd.DataFrame(resposta_json['jams'])
    if not df.empty:
        df['Pais'] = "BR"
        df['Cidade'] = "NITEROI"
        df['Vel_km_h'] = df['speedKMH']
        df['Tipo_da_rua'] = df['roadType']

        now = datetime.now()
        df['Data'] = now.strftime(format='%d/%m/%Y %H:%M')
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True) #+ timedelta(hours=3)
        df['Data'] = df['Data'].apply(lambda x: int(x.timestamp() * 1000))

    return df

# Função para enviar e-mail com o log de erros
def enviar_email(log_erros):
    try:
        if not log_erros:
            return
        corpo_email = "<br>".join(f"<p>{erro}</p>" for erro in log_erros)
        msg = MIMEText(corpo_email, 'html')
        msg['Subject'] = "Erro_Script_Live_Waze_Traffic"
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = EMAIL_DESTINATARIO
        
        with SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_REMETENTE, EMAIL_SENHA)
            smtp.sendmail(msg['From'], msg['To'], msg.as_string())
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")

# Função principal
def main():
    log_erros = []
    
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("EMAIL_PASSWORD")
    
    url = "https://www.waze.com/row-partnerhub-api/partners/18863635230/waze-feeds/feef83ff-1b4f-4d93-9d7a-dd3ea11304d3?format=1"
    
    resposta_json = obter_dados_api(url)
    
    if isinstance(resposta_json, str):
        log_erros.append(resposta_json)
        enviar_email(log_erros)
        return
    
    df = processar_dados(resposta_json)
    if isinstance(df, str):
        log_erros.append(df)
        enviar_email(log_erros)
        return
    
    try:
        gis = GIS("https://www.arcgis.com", USERNAME, PASSWORD)
        layer_id = LAYER_ID
        portal_item = gis.content.get(layer_id)
        traffic_agol = portal_item.layers[2]
        
    except Exception as e:
        log_erros.append(f"Erro ao conectar no ArcGIS: {e}")
        enviar_email(log_erros)
        return
    
    registros_removidos = limpar_registros_antigos(traffic_agol)
    if isinstance(registros_removidos, str):
        log_erros.append(registros_removidos)
    
    registros_adicionados = adicionar_registros(df, traffic_agol)
    if isinstance(registros_adicionados, str):
        log_erros.append(registros_adicionados)
    
    enviar_email(log_erros)

if __name__ == "__main__":
    main()