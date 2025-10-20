from prefect.variables import Variable
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.blocks.system import Secret
from dotenv import load_dotenv
import pandas as pd
import os
from tasks.shared import (
    get_layer_byId,
    add_features_to_layer,
    create_features,
    remove_features_from_layer,
    update_features,
    query_layer_dataframe,
)
from utils.index import compare_attributes, create_ms_timestamp
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import numpy as np

load_dotenv()

IS_LOCAL_RUN = os.getenv("ENVIRONMENT") == "development"

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
GIS_PORTAL_URL = "https://sig.niteroi.rj.gov.br/portal"
GIS_PORTAL_USERNAME = (
    os.getenv("GIS_PORTAL_USERNAME")
    if IS_LOCAL_RUN
    else Secret.load("usuario-pmngeo-portal").get()["username"]
)
GIS_PORTAL_PASSWORD = (
    os.getenv("GIS_PORTAL_PASSWORD")
    if IS_LOCAL_RUN
    else Secret.load("usuario-pmngeo-portal").get()["password"]
)
ALERTS_FEATURE_LAYER_ID = (
    os.getenv("ALERTS_FEATURE_LAYER_ID")
    if IS_LOCAL_RUN
    else Variable.get("waze_live_layer_id_agol")["ALERTS"]
)
ALERTS_FEATURE_LAYER_INDEX = 0

URL_ACCIDENT_HIST_PORTAL = (
    os.getenv("URL_ACIDENTES")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_ACIDENTES"]
)
URL_POT_HOLE_HIST_PORTAL = (
    os.getenv("URL_BURACOS")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_BURACOS"]
)
URL_CAR_STOPPED_HIST_PORTAL = (
    os.getenv("URL_CARRO_PARADO")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_CARRO_PARADO"]
)
URL_TRAFFIC_LIGHT_FAULT_HIST_PORTAL = (
    os.getenv("URL_FALHA_SEMAFORO")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_FALHA_SEMAFORO"]
)
URL_CONSTRUCTION_HIST_PORTAL = (
    os.getenv("URL_OBRA")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_OBRA"]
)
URL_LANE_CLOSED_HIST_PORTAL = (
    os.getenv("URL_VIA_FECHADA")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_VIA_FECHADA"]
)
URL_POLICE_HIST_PORTAL = (
    os.getenv("URL_POLICIA")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_POLICIA"]
)
URL_FLOOD_HIST_PORTAL = (
    os.getenv("URL_ALAGAMENTO")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_ALAGAMENTO"]
)
URL_HAZARD_ROAD_HIST_PORTAL = (
    os.getenv("URL_PERIGO_PISTA")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_PERIGO_PISTA"]
)
URL_HAZARD_OBJECT_HIST_PORTAL = (
    os.getenv("URL_OBJETO_PISTA")
    if IS_LOCAL_RUN
    else Variable.get("waze_hist_portal_layers")["URL_OBJETO_PISTA"]
)


@task(
    name="Atualiza features na camada live",
    description="Seleciona no df_live as features que estão na live e na api , cria features com valores novos, atualiza features na camada live",
    cache_policy=NO_CACHE,
)
def matching_att(df_live_layer, compared_data, matching_attributes, live_layer):

    logger = get_run_logger()
    logger.info(f"{matching_att.description}")

    try:
        live_matching = df_live_layer[
            df_live_layer["tx_uuid"].isin(compared_data["matching_attributes"])
        ]
        features_to_update = []

        for _, row_api in matching_attributes.iterrows():
            for _, row_live in live_matching.iterrows():
                if row_live["tx_uuid"] == row_api["tx_uuid"]:
                    feature = {
                        "attributes": {
                            **row_api.to_dict(),
                            "OBJECTID": row_live["OBJECTID"],
                        },
                    }
                    features_to_update.append(feature)

        update_features(live_layer, features_to_update)

    except Exception as e:
        error_message = str(e)
        raise ValueError(
            f"Erro durante a execução matching_attributes: {error_message}"
        )


@task(
    name="Remove da live e insere hist",
    description="Insere data de saída, trata dados para formato de histórico, filtra por contexto, insere as features na hist e excluí da live",
    cache_policy=NO_CACHE,
)
def create_hist(df_layer, live_layer):

    logger = get_run_logger()
    logger.info(f"{create_hist.description}")

    try:
        create_ms_timestamp(df_layer, "dt_saida")
        create_ms_timestamp(df_layer, "dt_data_hora")
        df_layer["dt_entrada"] = df_layer["dt_entrada"].replace({np.nan: None})
        df_layer.drop(columns=["OBJECTID"], axis=1, inplace=True)

        df_map = {
            "acidentes": pd.DataFrame(
                df_layer[df_layer["tx_tipo_alerta"] == "Acidente"]
            ),
            "buracos": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Buraco"]
            ),
            "semaforo": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Falha no semáforo"]
            ),
            "carro_parado": pd.DataFrame(
                df_layer[
                    df_layer["tx_subtipo_alerta"].isin(
                        ["Carro parado na pista", "Carro parado no acostamento"]
                    )
                ]
            ),
            "obras": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Obra na pista"]
            ),
            "via_fechada": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Uma via fechada"]
            ),
            "policia": pd.DataFrame(df_layer[df_layer["tx_tipo_alerta"] == "Polícia"]),
            "alagamento": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Alagamento"]
            ),
            "perigo_pista": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Perigo na pista"]
            ),
            "objeto_pista": pd.DataFrame(
                df_layer[df_layer["tx_subtipo_alerta"] == "Objeto na pista"]
            ),
        }

        url_map = {
            "acidentes": URL_ACCIDENT_HIST_PORTAL,
            "buracos": URL_POT_HOLE_HIST_PORTAL,
            "semaforo": URL_TRAFFIC_LIGHT_FAULT_HIST_PORTAL,
            "carro_parado": URL_CAR_STOPPED_HIST_PORTAL,
            "obras": URL_CONSTRUCTION_HIST_PORTAL,
            "via_fechada": URL_LANE_CLOSED_HIST_PORTAL,
            "policia": URL_POLICE_HIST_PORTAL,
            "alagamento": URL_FLOOD_HIST_PORTAL,
            "perigo_pista": URL_HAZARD_ROAD_HIST_PORTAL,
            "objeto_pista": URL_HAZARD_OBJECT_HIST_PORTAL,
        }

        for df_nome, df in df_map.items():
            PORTAL_URL = None
            for url_nome, url in url_map.items():
                if url_nome == df_nome:
                    PORTAL_URL = url

            if len(df) > 0:
                logger.info(f"{df_nome}: {len(df)}")
                feats = create_features(df)
                GIS(
                    GIS_PORTAL_URL,
                    GIS_PORTAL_USERNAME,
                    GIS_PORTAL_PASSWORD,
                    verify_cert=False,
                )
                portal_layer = FeatureLayer(PORTAL_URL)
                add_features_to_layer(portal_layer, feats)

        remove_features_from_layer(live_layer, df_layer)

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução only_in_layer: {error_message}")


@task(
    name="Filtro de alertas",
    description="Filtra alertas que devem ser adicionados, removidos ou atualizados",
    cache_policy=NO_CACHE,
)
def alerts_filter(df_api, df_live_layer, live_layer):
    try:

        # Compara "tx_uuid"'s vindos da API com os "uuid" que já estão na camada live
        compared_data = compare_attributes(df_api, "tx_uuid", df_live_layer, "tx_uuid")

        # Cria uma "Serie" com apenas os itens que estão presentes no dataframe da API
        only_in_API = (
            df_api[df_api["tx_uuid"].isin(compared_data["only_in_df"])]
            if len(compared_data["only_in_df"]) > 0
            else pd.DataFrame([])
        )

        # Cria uma "Serie" com os itens que estão presentes simultaneamente no dataframe da API e na camada live
        matching_attributes = (
            df_api[df_api["tx_uuid"].isin(compared_data["matching_attributes"])]
            if len(compared_data["matching_attributes"]) > 0
            else pd.DataFrame([])
        )

        # Cria uma "Serie" com os itens que estão presentes apenas na camada live
        only_in_layer = (
            df_live_layer[df_live_layer["tx_uuid"].isin(compared_data["only_in_layer"])]
            if len(compared_data["only_in_layer"]) > 0
            else pd.DataFrame([])
        )

        # Quando os itens comparados estiverem presentes apenas no dataframe da API devem ser incluídos na camada live
        # com preenchendo a data de criação no campo startTime
        if only_in_API is not None and not only_in_API.empty:
            create_ms_timestamp(only_in_API, "dt_entrada")
            feature_list = create_features(only_in_API)
            add_features_to_layer(live_layer, feature_list)

        # Quando os itens comparados estiverem presentes apenas na camada live devem ser excluídos da camada live
        # e incluídos na camada de histórico preenchendo o valor dt_saída com a data referente a exclusão
        if only_in_layer is not None and not only_in_layer.empty:
            create_hist(only_in_layer, live_layer)

        # Quando os itens comparados estiverem presentes em ambos os dataframes o valor do campo "Atualizado" deve ser preenchido com a data atual
        if matching_attributes is not None and not matching_attributes.empty:
            matching_att(df_live_layer, compared_data, matching_attributes, live_layer)

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")


@flow(
    name="Alertas",
    description="Fluxo para manter camadas de alertas live e hist atualizadas",
    log_prints=True,
)
def waze_alerts(df_transformed_api_data):
    try:
        logger = get_run_logger()
        logger.info(f"{waze_alerts.description}")
        gis = GIS(GIS_AGOL_URL, GIS_AGOL_USERNAME, GIS_AGOL_PASSWORD)

        alerts_feature_layer = get_layer_byId(
            gis, ALERTS_FEATURE_LAYER_ID, ALERTS_FEATURE_LAYER_INDEX
        )

        df_fl_alerts = query_layer_dataframe(alerts_feature_layer)

        alerts_filter(df_transformed_api_data, df_fl_alerts, alerts_feature_layer)

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")
