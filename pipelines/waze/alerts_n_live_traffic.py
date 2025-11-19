from prefect import flow
from prefect.blocks.system import Secret
from prefect import task, get_run_logger
from dotenv import load_dotenv
import requests
import pandas as pd
import os
from utils.index import df_col_type_mapping, create_ms_timestamp
from alerts_flow import waze_alerts
from traffic_flow import waze_traffic
from tasks.collect_waze import get_waze_partnerhub_json

load_dotenv()

IS_LOCAL_RUN = os.getenv("ENVIRONMENT") == "development"

WAZE_PARTNER_HUB_API_URL = (
    os.getenv("WAZE_PARTNER_HUB_API_URL")
    if IS_LOCAL_RUN
    else Secret.load("waze-url-autenticada-api").get()
)

RAW_FILES_PATH = "/opt/prefect_raw/data"


MAP_TRAFFIC_COLLUMS = {
    "country": "tx_pais",
    "city": "tx_cidade",
    "length": "li_comprimento",
    "endNode": "tx_final",
    "speedKMH": "db_velocidade_kmh",
    "speed": "db_velocidade",
    "delay": "li_atraso",
    "level": "li_nivel",
    "id": "li_id",
}

MAP_SHARED_COLUMNS = {
    "uuid": "tx_uuid",
    "street": "tx_rua",
    "roadType": "tx_tipo_via",
}

MAP_ALERTS_COLUMNS = {
    "reportRating": "li_avaliacao_informe",
    "reportByMunicipalityUser": "tx_informe_municipal",
    "confidence": "li_confianca",
    "reliability": "li_confiabilidade",
    "type": "tx_tipo_alerta",
    "magvar": "li_direcao_graus",
    "subtype": "tx_subtipo_alerta",
}

MAP_TIPO_VIA = {
    1: "Rua",
    2: "Rua principal",
    3: "Via expressa",
    4: "Rampa de acesso",
    5: "Trilha",
    6: "Principal",
    7: "Secundária",
    8: "Trilha 4X4",
    14: "Trilha 4X4",
    15: "Travessia de balsa",
    9: "Passarela",
    10: "Passagem para pedestres",
    11: "Saída",
    16: "Escadaria",
    17: "Via particular",
    18: "Ferrovia",
    19: "Pista de pouso, decolagem e taxiamento",
    20: "Via de estacionamento",
    21: "Via de serviço",
}


@task(
    name="Transform alerts data",
    description="Prepara dados de alerta para o mesmo padrão da camada",
)
def transform_alerts(api_data):
    try:
        df_alerts = pd.DataFrame(api_data)
        create_ms_timestamp(df_alerts, "dt_data_hora")
        df_alerts = df_alerts.rename(
            columns={**MAP_SHARED_COLUMNS, **MAP_ALERTS_COLUMNS}
        )
        df_alerts["tx_tipo_via"] = df_alerts["tx_tipo_via"].map(MAP_TIPO_VIA)
        df_alerts["tx_tipo_alerta"] = df_alerts["tx_tipo_alerta"].replace(
            {
                "ACCIDENT": "Acidente",
                "HAZARD": "Perigo",
                "ROAD_CLOSED": "Via interditada",
                "JAM": "Trânsito",
                "POLICE": "Polícia",
            }
        )
        df_alerts["tx_subtipo_alerta"] = df_alerts["tx_subtipo_alerta"].replace(
            {
                "ACCIDENT_MINOR": "Acidente leve",
                "ACCIDENT_MAJOR": "Acidente grave",
                "HAZARD_ON_ROAD": "Perigo na pista",
                "HAZARD_ON_ROAD_EMERGENCY_VEHICLE": "Veículo de emergência na pista",
                "HAZARD_ON_ROAD_CAR_STOPPED": "Carro parado na pista",
                "HAZARD_ON_ROAD_CONSTRUCTION": "Obra na pista",
                "HAZARD_ON_ROAD_ICE": "Gelo na pista",
                "HAZARD_ON_ROAD_LANE_CLOSED": "Uma via fechada",
                "HAZARD_ON_ROAD_OBJECT": "Objeto na pista",
                "HAZARD_ON_ROAD_OIL": "Óleo na pista",
                "HAZARD_ON_ROAD_POT_HOLE": "Buraco",
                "HAZARD_ON_ROAD_ROAD_KILL": "Morte na pista",
                "HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT": "Falha no semáforo",
                "HAZARD_ON_SHOULDER": "Atenção no acostamento",
                "HAZARD_ON_SHOULDER_ANIMALS": "Animal no acostamento",
                "HAZARD_ON_SHOULDER_CAR_STOPPED": "Carro parado no acostamento",
                "HAZARD_ON_SHOULDER_MISSING_SIGN": "Ausência de sinalização",
                "HAZARD_WEATHER": "Risco de temporal",
                "HAZARD_WEATHER_FLOOD": "Alagamento",
                "HAZARD_WEATHER_FOG": "Nevoeiro",
                "HAZARD_WEATHER_FREEZING_RAIN": "Chuva congelante",
                "HAZARD_WEATHER_HAIL": "Chuva de granizo",
                "HAZARD_WEATHER_HEAT_WAVE": "Onda de calor",
                "HAZARD_WEATHER_HEAVY_RAIN": "Chuva intensa",
                "HAZARD_WEATHER_HEAVY_SNOW": "Neve",
                "HAZARD_WEATHER_HURRICANE": "Furacão",
                "HAZARD_WEATHER_MONSOON": "Monção",
                "HAZARD_WEATHER_TORNADO": "Tornado",
                "ROAD_CLOSED_HAZARD": "Perigo na pista",
                "ROAD_CLOSED_CONSTRUCTION": "Obra na pista",
                "ROAD_CLOSED_EVENT": "Interdição eventual",
                "JAM_LIGHT_TRAFFIC": "Trânsito leve",
                "JAM_MODERATE_TRAFFIC": "Trânsito moderado",
                "JAM_HEAVY_TRAFFIC": "Trânsito intenso",
                "JAM_STAND_STILL_TRAFFIC": "Trânsito parado",
                "POLICE_VISIBLE": "Policiamento",
                "POLICE_HIDING": "Policiamento",
            }
        )
        df_alerts["db_lat"] = df_alerts["location"].apply(
            lambda x: x.get("y") if isinstance(x, dict) else None
        )
        df_alerts["db_long"] = df_alerts["location"].apply(
            lambda x: x.get("x") if isinstance(x, dict) else None
        )
        df_alerts.drop(
            columns=["pubMillis", "location", "city", "country"], axis=1, inplace=True
        )
        df_col_type_mapping(df_alerts)

        return df_alerts

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dados api alerts: {error_message}")


@task(
    name="Transform traffic data",
    description="Prepara dados de tráfego para o mesmo padrão da camada",
)
def transform_traffic(api_data):

    try:
        df_traffic = pd.DataFrame(api_data)
        df_traffic = df_traffic.rename(
            columns={**MAP_TRAFFIC_COLLUMS, **MAP_SHARED_COLUMNS}
        )
        create_ms_timestamp(df_traffic, "dt_data_hora")
        df_traffic["tx_tipo_via"] = df_traffic["tx_tipo_via"].map(MAP_TIPO_VIA)
        df_traffic.drop(columns=["pubMillis", "segments"], axis=1, inplace=True)
        df_col_type_mapping(df_traffic)

        return df_traffic

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dados api traffic: {error_message}")


@task(
    name="Transform api data",
    description="Prepara dados vindos da api com o mesmo padrão da camada e cria dataframe",
)
def transform_api_data(data):

    logger = get_run_logger()
    logger.info(f"{transform_api_data.description}")

    try:
        if "alerts" in data and isinstance(data["alerts"], list) and data["alerts"]:
            df_api_alerts = transform_alerts(data["alerts"])
        else:
            df_api_alerts = pd.DataFrame([])

        if "jams" in data and isinstance(data["jams"], list) and data["jams"]:
            df_api_traffic = transform_traffic(data["jams"])
        else:
            df_api_traffic = pd.DataFrame([])

        logger.info("Sucesso ao preparar dados da api")

        return {"alerts": df_api_alerts, "traffic": df_api_traffic}

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dataframe: {error_message}")


def compare_attributes(df, df_attribute, layer, layer_attribute):
    try:
        df_attributes = set(df[df_attribute])

        if len(layer) > 0:
            layer_attributes = set(layer[layer_attribute])
            matching_attributes = df_attributes.intersection(layer_attributes)
            only_in_df = df_attributes - layer_attributes
            only_in_layer = layer_attributes - df_attributes
            print(
                f"matching_attributes: {len(matching_attributes)} registros",
                f"only_in_df: {len(only_in_df)} registros",
                f"only_in_layer: {len(only_in_layer)} registros",
            )

            return {
                "matching_attributes": matching_attributes,
                "only_in_df": only_in_df,
                "only_in_layer": only_in_layer,
            }

        else:
            return {
                "matching_attributes": [],
                "only_in_df": df_attributes,
                "only_in_layer": [],
            }

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a comparação: {error_message}")


@task(
    name="Cria features com data de entrada",
    description="Insere data de entrada e cria features georreferenciadas",
)
def create_features(df_api):

    logger = get_run_logger()
    logger.info(f"{create_features.description}")

    try:
        create_ms_timestamp(df_api, "dt_entrada")
        features_to_add = []

        for _, row in df_api.iterrows():
            new_feature = {
                "attributes": row.to_dict(),
                "geometry": {
                    "x": float(row["db_long"]),
                    "y": float(row["db_lat"]),
                    "spatialReference": {"wkid": 4674},
                },
            }
            features_to_add.append(new_feature)

        return features_to_add
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução only_in_api: {error_message}")


@flow(
    name="Waze Live",
    description="Fluxo para manter camadas atualizada com api waze",
    log_prints=True,
)
def waze_live():
    try:
        logger = get_run_logger()
        logger.info(f"{waze_live.description}")
        waze_json_data = get_waze_partnerhub_json(
            WAZE_PARTNER_HUB_API_URL, RAW_FILES_PATH
        )
        transformed_api_data = transform_api_data(waze_json_data)

        if not transformed_api_data["traffic"].empty:
            waze_traffic(transformed_api_data["traffic"])

        if not transformed_api_data["alerts"].empty:
            waze_alerts(transformed_api_data["alerts"])

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    waze_live()
