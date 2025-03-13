import pandas as pd
import datetime

map_column_names = {
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

map_tipo_via = {
    1: 'Rua',
    2: 'Rua principal',
    3: 'Via expressa',
    4: 'Rampa de acesso',
    5: 'Trilha',
    6: 'Principal',
    7: 'Secundária',
    8: 'Trilha 4X4',
    14: 'Trilha 4X4',
    15: 'Travessia de balsa',
    9: 'Passarela',
    10: 'Passagem para pedestres',
    11: 'Saída',
    16: 'Escadaria',
    17: 'Via particular',
    18: 'Ferrovia',
    19: 'Pista de pouso, decolagem e taxiamento',
    20: 'Via de estacionamento',
    21: 'Via de serviço'
}


def create_ms_timestamp(df,col_name):
    now = datetime.datetime.now() + datetime.timedelta(hours=3) 
    df[col_name] = now.strftime(format='%d/%m/%Y %H:%M')
    df[col_name] = pd.to_datetime(df[col_name], dayfirst=True).apply(lambda x: int(x.timestamp() * 1000))
    


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


def parse_api_data(data):
    df_alerts = pd.DataFrame(data['alerts'])
    df_alerts['datetime'] = pd.to_datetime(df_alerts['pubMillis'], unit='ms')

    df_alerts['Pais'] = df_alerts['country'].astype(str)
    df_alerts['Cidade'] = df_alerts['city'].astype(str)
    df_alerts['reportRating'] = df_alerts['reportRating'].astype(int)
    df_alerts['confidence'] = df_alerts['confidence'].astype(int)
    df_alerts['reliability'] = df_alerts['reliability'].astype(int)
    df_alerts['Rua'] = df_alerts['street'].astype(str)
    df_alerts['Lat'] = df_alerts['location'].apply(lambda x: x.get('y') if isinstance(x, dict) else None)
    df_alerts['Lng'] = df_alerts['location'].apply(lambda x: x.get('x') if isinstance(x, dict) else None)
    df_alerts['Tipo_da_rua'] = df_alerts['roadType']
    df_alerts['Atualizado'] = datetime.datetime.now() + datetime.timedelta(hours=3)   

    df_alerts['Tipo'] = df_alerts['type'].astype(str).replace({
        'ACCIDENT': 'Acidente', 'HAZARD': 'Perigo',
        'ROAD_CLOSED': 'Via interditada', 'JAM': 'Trânsito', 'POLICE': 'Polícia'
    })

    df_alerts['Subtipo'] = df_alerts['subtype'].astype(str).replace({

    'ACCIDENT_MINOR': 'Acidente leve', 'ACCIDENT_MAJOR': 'Acidente grave',
    'HAZARD_ON_ROAD': 'Perigo na pista', 'HAZARD_ON_ROAD_EMERGENCY_VEHICLE': 'Veículo de emergência na pista',
    'HAZARD_ON_ROAD_CAR_STOPPED': 'Carro parado na pista', 'HAZARD_ON_ROAD_CONSTRUCTION': 'Obra na pista',
    'HAZARD_ON_ROAD_ICE': 'Gelo na pista','HAZARD_ON_ROAD_LANE_CLOSED': 'Uma via fechada',
    'HAZARD_ON_ROAD_OBJECT': 'Objeto na pista', 'HAZARD_ON_ROAD_OIL': 'Óleo na pista',
    'HAZARD_ON_ROAD_POT_HOLE': 'Buraco', 'HAZARD_ON_ROAD_ROAD_KILL': 'Morte na pista',
    'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT': 'Falha no semáforo', 'HAZARD_ON_SHOULDER': 'Atenção no acostamento',
    'HAZARD_ON_SHOULDER_ANIMALS': 'Animal no acostamento', 'HAZARD_ON_SHOULDER_CAR_STOPPED': 'Carro parado no acostamento',
    'HAZARD_ON_SHOULDER_MISSING_SIGN': 'Ausência de sinalização', 'HAZARD_WEATHER': 'Risco de temporal',
    'HAZARD_WEATHER_FLOOD': 'Alagamento', 'HAZARD_WEATHER_FOG': 'Nevoeiro', 'HAZARD_WEATHER_FREEZING_RAIN': 'Chuva congelante',
    'HAZARD_WEATHER_HAIL': 'Chuva de granizo', 'HAZARD_WEATHER_HEAT_WAVE': 'Onda de calor',
    'HAZARD_WEATHER_HEAVY_RAIN': 'Chuva intensa', 'HAZARD_WEATHER_HEAVY_SNOW': 'Neve',
    'HAZARD_WEATHER_HURRICANE': 'Furacão', 'HAZARD_WEATHER_MONSOON': 'Monção',
    'HAZARD_WEATHER_TORNADO': 'Tornado', 'ROAD_CLOSED_HAZARD': 'Perigo na pista',
    'ROAD_CLOSED_CONSTRUCTION': 'Obra na pista', 'ROAD_CLOSED_EVENT': 'Interdição eventual',
    'JAM_LIGHT_TRAFFIC': 'Trânsito leve', 'JAM_MODERATE_TRAFFIC': 'Trânsito moderado',
    'JAM_HEAVY_TRAFFIC': 'Trânsito intenso', 'JAM_STAND_STILL_TRAFFIC': 'Trânsito parado',
    'POLICE_VISIBLE': 'Policiamento', 'POLICE_HIDING': 'Policiamento'
    })

    df_alerts.drop(columns=['subtype', 'type','roadType', 'location', 'city', 'country','reportByMunicipalityUser'], axis=1, inplace=True)

    return df_alerts
