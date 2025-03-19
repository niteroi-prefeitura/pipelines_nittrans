import pandas as pd
import datetime
import numpy as np
from prefect import task

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
        'magvar': 'db_direcao_graus',          
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


def create_ms_timestamp(df,col_name):
    now = datetime.datetime.now() + datetime.timedelta(hours=3) 
    df[col_name] = now.strftime(format='%d/%m/%Y %H:%M')
    df[col_name] = pd.to_datetime(df[col_name], dayfirst=True).apply(lambda x: int(x.timestamp() * 1000))

@task(name="Parse api data", description="Prepara dados vindos da api com o mesmo padrão da camada e cria dataframe")
def parse_api_data(data):
    try:
        df_alerts = pd.DataFrame(data['alerts'])
        df_alerts['datetime'] = pd.to_datetime(df_alerts['pubMillis'], unit='ms')

        df_alerts['Pais'] = df_alerts['country'].astype(str)
        df_alerts['Cidade'] = df_alerts['city'].astype(str)
        df_alerts['reportRating'] = df_alerts['reportRating'].astype(int)
        df_alerts['confidence'] = df_alerts['confidence'].astype(int)
        df_alerts['reliability'] = df_alerts['reliability'].astype(int)
        df_alerts['reportByMunicipalityUser'] = df_alerts['reportByMunicipalityUser'].astype(str)
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

        df_alerts.drop(columns=['subtype', 'type','roadType', 'location', 'city', 'country'], axis=1, inplace=True)

        return df_alerts
    
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a execução parse_api_data: {error_message}")

@task(name="Parse hist data", description="Prepara dados vindos da live para formato utilizado na camada hist e cria DF")
def parse_hist_data(data):
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
        print(f"Erro durante a execução parse_hist_data: {error_message}")