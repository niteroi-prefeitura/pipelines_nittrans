import pandas as pd
import numpy as np
from utils.index import create_ms_timestamp
from prefect import task, get_run_logger

map_hist_column_names = {
        'uuid': 'tx_uuid',
        'Rua': 'tx_rua',
        'Tipo_da_rua': 'tx_tipo_via',
        'Tipo': 'tx_tipo_alerta',
        'Subtipo': 'tx_subtipo_alerta',
        'reportRating': 'db_avaliacao_informe',
        'reportByMunicipalityUser': 'tx_informe_municipal',
        'confidence': 'db_confianca',
        'reliability': 'db_confiabilidade',
        'Atualizado': 'dt_data_hora',
        'Magvar': 'db_direcao_graus',          
        'startTime': 'dt_entrada',
##
        'endTime': 'dt_saida',        
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

map_shared_columns = {
    'uuid': 'tx_uuid',
    'street': 'tx_rua',
    'roadType':'tx_tipo_via',
}
map_alerts_columns = {
    'reportRating': 'db_avaliacao_informe',
    'reportByMunicipalityUser': 'tx_informe_municipal',
    'confidence': 'db_confianca',
    'reliability': 'db_confiabilidade',
    'type': 'tx_tipo_alerta',
    'magvar': 'db_direcao_graus',
    'subtype': 'tx_subtipo_alerta'
}
map_traffic_columns = {
    'country': 'tx_pais',
    'city': 'tx_cidade',
    'length': 'li_comprimento',
    'endNode': 'tx_final',
    'speedKMH': 'db_velocidade_kmh',
    'speed': 'db_velocidade',
    'delay': 'li_atraso',
    'level': 'li_nivel',
    'id': 'li_id',      
}

@task(name="Parse api data", description="Prepara dados vindos da api com o mesmo padrão da camada e cria dataframe")
def parse_api_data(data):

    logger = get_run_logger()
    logger.info(f"{parse_api_data.description}")

    try:

        df_api_alerts = parse_live_alerts(data['alerts'])
        df_api_traffic = parse_traffic_live_data(data['jams'])
        
        
        logger.info('Sucesso ao preparar dados da api')
        
        return {
            "alerts": df_api_alerts, 
            "traffic":df_api_traffic
            }
        
    
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução parse_api_data: {error_message}")

def parse_live_alerts(api_data):
    try:
        df_alerts = pd.DataFrame(api_data)
        create_ms_timestamp(df_alerts, 'dt_data_hora')
        df_alerts = df_alerts.rename(columns={**map_shared_columns, **map_alerts_columns}) 
        df_alerts['tx_tipo_via'] = df_alerts['tx_tipo_via'].map(map_tipo_via)
        df_alerts['tx_uuid'] = df_alerts['tx_uuid'].astype(str)
        df_alerts['tx_tipo_alerta'] = df_alerts['tx_tipo_alerta'].astype(str).replace({
            'ACCIDENT': 'Acidente', 'HAZARD': 'Perigo',
            'ROAD_CLOSED': 'Via interditada', 'JAM': 'Trânsito', 'POLICE': 'Polícia'
        })
        df_alerts['tx_subtipo_alerta'] = df_alerts['tx_subtipo_alerta'].astype(str).replace({

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
        df_alerts['db_lat'] = df_alerts['location'].apply(lambda x: x.get('y') if isinstance(x, dict) else None)
        df_alerts['db_long'] = df_alerts['location'].apply(lambda x: x.get('x') if isinstance(x, dict) else None) 
        df_alerts.drop(columns=['pubMillis','location', 'city', 'country'], axis=1, inplace=True)

        return df_alerts
    
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dados api alerts: {error_message}")

@task(name="Parse hist data", description="Prepara dados vindos da live para formato utilizado na camada hist e cria DF")
def parse_hist_data(data):

    logger = get_run_logger()
    logger.info(f"{parse_hist_data.description}")

    try:
        df_hist = pd.DataFrame(data)
        df_hist = df_hist.rename(columns=map_hist_column_names)   
        df_hist.drop(columns=['OBJECTID', 'Cidade', 'Pais', 'Pubmillis', 'endTimeMillis', 'startTimeMillis'], axis=1, inplace=True) 
        df_hist['tx_tipo_via'] = df_hist['tx_tipo_via'].map(map_tipo_via)
        df_hist['dt_entrada'] = df_hist['dt_entrada'].replace({np.nan: None})
        create_ms_timestamp(df_hist,'dt_data_hora')
        return df_hist
    
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução parse_hist_data: {error_message}")
    
def parse_traffic_live_data(api_data):

    try:
        df_traffic = pd.DataFrame(api_data)
        df_traffic = df_traffic.rename(columns={**map_traffic_columns,**map_shared_columns
        })
        create_ms_timestamp(df_traffic,'dt_data_hora')
        df_traffic['tx_tipo_via'] = df_traffic['tx_tipo_via'].map(map_tipo_via)
        df_traffic['tx_uuid'] = df_traffic['tx_uuid'].astype(str)        
        df_traffic.drop(columns=['pubMillis','segments'], axis=1, inplace=True)

        return df_traffic

    except Exception as e:
            error_message = str(e)
            raise ValueError(f"Erro ao preparar dados api traffic: {error_message}")