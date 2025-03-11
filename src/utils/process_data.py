import pandas as pd
from datetime import datetime, timedelta


def process_data_live_traffic(data):
    df = pd.DataFrame(data['jams'])

    if df.empty:
        raise ValueError(f"Erro no processamento de dados: {df}")

    df['Pais'] = "BR"
    df['Cidade'] = "NITEROI"
    df['Vel_km_h'] = df['speedKMH']
    df['Tipo_da_rua'] = df['roadType']

    now = datetime.now()
    df['Data'] = now.strftime(format='%d/%m/%Y %H:%M')
    df['Data'] = pd.to_datetime(df['Data'], dayfirst=True)
    df['Data'] = df['Data'].apply(lambda x: int(x.timestamp() * 1000))

    return df


def process_data_waze_alerts(data):
    df_alerts = pd.DataFrame(data['alerts'])
    df_alerts['datetime'] = pd.to_datetime(df_alerts['pubMillis'], unit='ms')
    df_alerts = df_alerts.drop(['country', 'city', 'pubMillis'], axis=1)

    # Renomear as colunas
    new_column_names = {
        'id': 'id_alerta',
        'type': 'tx_tipo_alerta',
        'subtype': 'tx_subtipo_alerta',
        'location': 'tx_localizacao',
        'city': 'tx_cidade',
        'street': 'tx_rua',
        'magnitude': 'db_magnitude',
        'reportRating': 'db_avaliacao_informe',
        'reportDescription': 'tx_descricao_informe',
        'reportType': 'tx_tipo_informe',
        'reportCategory': 'tx_categoria_informe',
        'thumbsUpCount': 'db_qtde_curtidas',
        'speed': 'db_velocidade',
        'reliability': 'db_confiabilidade',
        'roadType': 'tx_tipo_via',
        'severity': 'db_severidade',
        'startTime': 'dt_inicio',
        'endTime': 'dt_fim',
        'imageUrl': 'tx_url_imagem',
        'uuid': 'tx_uuid',
        'reportByMunicipalityUser': 'tx_informe_municipal',
        'confidence': 'db_confianca',
        'magvar': 'db_direcao_graus',
        'country': 'tx_pais',
        'region': 'tx_regiao',
        'longitude': 'db_longitude',
        'latitude': 'db_latitude',
        'datetime': 'dt_data_hora'
    }

    df_alerts = df_alerts.rename(columns=new_column_names)

    # Extrair latitude e longitude de 'tx_localizacao'
    df_alerts['db_latitude'] = df_alerts['tx_localizacao'].apply(
        lambda x: x.get('y') if isinstance(x, dict) else None)
    df_alerts['db_longitude'] = df_alerts['tx_localizacao'].apply(
        lambda x: x.get('x') if isinstance(x, dict) else None)

    df_alerts = df_alerts.drop('tx_localizacao', axis=1)
    return df_alerts
