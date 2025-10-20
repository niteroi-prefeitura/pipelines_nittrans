from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
from arcgis.gis import GIS
from arcgis.geometry import Polyline
from dotenv import load_dotenv
import os
from tasks.shared import (
    query_layer_dataframe,
    get_layer_byId,
    add_features_to_layer,
    remove_features_from_layer,
)

load_dotenv()

IS_LOCAL_RUN = os.getenv("ENVIRONMENT") == "development"

WAZE_PARTNER_HUB_API_URL = (
    os.getenv("WAZE_PARTNER_HUB_API_URL")
    if IS_LOCAL_RUN
    else Secret.load("waze-url-autenticada-api").get()
)
GIS_AGOL_URL = "https://www.arcgis.com"
GIS_AGOL_USERNAME = (
    os.getenv("GIS_USERNAME")
    if IS_LOCAL_RUN
    else Secret.load("usuario-integrador-agol").get()["username"]
)
GIS_AGOL_PASSWORD = (
    os.getenv("GIS_PASSWORD")
    if IS_LOCAL_RUN
    else Secret.load("usuario-integrador-agol").get()["password"]
)
TRAFFIC_FEATURE_LAYER_ID = (
    os.getenv("TRAFFIC_FEATURE_LAYER_ID")
    if IS_LOCAL_RUN
    else Variable.get("waze_live_layer_id_agol")["TRAFFIC"]
)
TRAFFIC_FEATURE_LAYER_INDEX = 0


@task(
    name="Atualizar Features",
    description="Atualiza camada de trafego live",
    cache_policy=NO_CACHE,
)
def update_feature_layer(df_api, df_live_layer, live_layer):

    logger = get_run_logger()
    logger.info(f"{update_feature_layer.description}")

    features_to_add = []

    try:
        for _, row in df_api.iterrows():

            paths = [[(point["x"], point["y"]) for point in row["line"]]]
            polyline = Polyline({"paths": paths, "spatialReference": {"wkid": 4326}})

            att = row.to_dict()
            del att["line"]

            feature_template = {"attributes": att, "geometry": polyline}
            features_to_add.append(feature_template)

        if not df_live_layer.empty:
            result = remove_features_from_layer(live_layer, df_live_layer)
            if result == True:
                add_features_to_layer(live_layer, features_to_add)
        else:
            add_features_to_layer(live_layer, features_to_add)

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dados de trafego : {error_message}")


@flow(
    name="Waze Tráfego Live",
    description="Fluxo para manter camada atualizada com api waze",
    log_prints=True,
)
def waze_traffic(df_transformed_api_data):
    try:
        logger = get_run_logger()
        logger.info(f"{waze_traffic.description}")

        gis = GIS(GIS_AGOL_URL, GIS_AGOL_USERNAME, GIS_AGOL_PASSWORD)

        traffic_feature_layer = get_layer_byId(
            gis, TRAFFIC_FEATURE_LAYER_ID, TRAFFIC_FEATURE_LAYER_INDEX
        )

        df_fl_live = query_layer_dataframe(traffic_feature_layer)

        update_feature_layer(df_transformed_api_data, df_fl_live, traffic_feature_layer)
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")
