from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
from arcgis.gis import GIS
from arcgis.geometry import Polyline
from dotenv import load_dotenv
import requests
import pandas as pd
import datetime
import pytz
import os

load_dotenv()

IS_LOCAL_RUN = os.getenv("ENVIRONMENT") == "development"

WAZE_PARTNER_HUB_API_URL = (
    os.getenv("WAZE_PARTNER_HUB_API_URL")
    if IS_LOCAL_RUN
    else Secret.load("waze-url-autenticada-api").get()
)
GIS_URL = "https://www.arcgis.com"
GIS_USERNAME = (
    os.getenv("GIS_USERNAME")
    if IS_LOCAL_RUN
    else Secret.load("usuario-integrador-agol").get()["username"]
)
GIS_PASSWORD = (
    os.getenv("GIS_PASSWORD")
    if IS_LOCAL_RUN
    else Secret.load("usuario-integrador-agol").get()["password"]
)
TRAFFIC_FEATURE_LAYER_ID = (
    os.getenv("TRAFFIC_FEATURE_LAYER_ID")
    if IS_LOCAL_RUN
    else Variable.get("waze_live_layer_id_agol").get()["TRAFFIC"]
)
TRAFFIC_FEATURE_LAYER_INDEX = 0

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
    name="Conectar ao GIS",
    description="Cria uma instância GIS com as credenciais fornecidas",
)
def create_gis_instance():
    logger = get_run_logger()
    logger.info(f"{create_gis_instance.description}")
    return GIS(GIS_URL, GIS_USERNAME, GIS_PASSWORD)


@task(
    name="Buscar dados", description="Busca dados da API Waze e retorna em formato JSON"
)
def get_api_data_as_json():
    logger = get_run_logger()
    logger.info(f"{get_api_data_as_json.description}")

    try:
        response = requests.get(WAZE_PARTNER_HUB_API_URL)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, dict):
            raise ValueError(f"Erro ao obter dados da API em JSON: {data}")

        logger.info("Dados obtidos com sucesso")

        return data

    except requests.exceptions.RequestException as e:
        raise ValueError(f"Erro ao acessar a API: {e}")


def type_mapping(type):
    type_dict = {
        "tx": "str",
        "db": "float",
        "li": "int",
    }
    return type_dict[type]


def create_ms_timestamp(df, col_name):
    brasil_tz = pytz.timezone("America/Sao_Paulo")
    now = datetime.datetime.now(brasil_tz)
    timestamp_ms = int(now.timestamp() * 1000)
    df[col_name] = timestamp_ms


def df_col_type_mapping(df):
    for col in df.columns:
        col_type = col.split("_")[0]
        if len(col.split("_")) > 1 and col_type != "dt":
            df[col] = df[col].astype(type_mapping(col_type))


@task(
    name="Parse api data",
    description="Prepara dados vindos da api com o mesmo padrão da camada e cria dataframe",
)
def parse_api_data_to_df(data):

    logger = get_run_logger()
    logger.info(f"{parse_api_data_to_df.description}")

    try:
        if "jams" in data and isinstance(data["jams"], list) and data["jams"]:
            df_traffic = pd.DataFrame(data["jams"])
            df_traffic = df_traffic.rename(
                columns={**MAP_TRAFFIC_COLLUMS, **MAP_SHARED_COLUMNS}
            )
            create_ms_timestamp(df_traffic, "dt_data_hora")
            df_traffic["tx_tipo_via"] = df_traffic["tx_tipo_via"].map(MAP_TIPO_VIA)
            df_traffic.drop(columns=["pubMillis", "segments"], axis=1, inplace=True)
            df_col_type_mapping(df_traffic)

        else:
            df_traffic = pd.DataFrame([])

        logger.info("Sucesso ao preparar dataframe")

        return df_traffic

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dataframe: {error_message}")


@task(
    name="Buscar camada",
    description="Consulta o id e índice fornecido e retorna a camada",
    cache_policy=NO_CACHE,
)
def get_layer_byId(gis, layer_id, layer_index):

    logger = get_run_logger()
    logger.info(f"{get_layer_byId.description}")

    try:
        portal_item = gis.content.get(layer_id)

        if not portal_item:
            raise Exception("Item não encontrado. Verifique o layer_id.")

        logger.info(f"Sucesso ao buscar camada id:{layer_id} e index:{layer_index}")

        return portal_item.layers[layer_index]

    except Exception as e:
        raise ValueError(f"Erro ao buscar camada: {e}")


@task(
    name="Criar Dataframe de Features",
    description="Constrói dataframe baseado nas features da camada live",
    cache_policy=NO_CACHE,
)
def query_layer(layer):

    logger = get_run_logger()
    logger.info(f"{query_layer.description}")

    attributes = "*"
    where = "1=1"
    existing_features_attributes = []

    try:
        existing_features = layer.query(where, out_fields=[attributes]).features

        for feat in existing_features:
            existing_features_attributes.append(feat.attributes)

        df_existing = pd.DataFrame(existing_features_attributes)

        logger.info("Sucesso ao criar Dataframe de Features em camada")

        logger.info(f"Amostra dos primeiros registros:\n{df_existing.head(10)}")

        return df_existing

    except Exception as e:
        raise ValueError(f"Erro ao pegar os atributos da layer: {e}")


@task(
    name="Remover features",
    description="Modifica camada removendo features",
    cache_policy=NO_CACHE,
)
def remove_feature_from_layer(layer, df):

    logger = get_run_logger()
    logger.info(f"{remove_feature_from_layer.description}")

    uuids_to_delete = df["tx_uuid"].tolist()

    uuids_formatados = [f"'{uuid}'" for uuid in uuids_to_delete]

    query = ", ".join(uuids_formatados)

    response = layer.delete_features(where=f"tx_uuid IN ({query})")

    if response["deleteResults"]:
        logger.info(f"{len(uuids_to_delete)} registros removidos.")
        return True
    else:
        raise ValueError(f"Falha na exclusão dos itens.")


@task(
    name="Adicionar features",
    description="Modifica camada adicionando features",
    cache_policy=NO_CACHE,
)
def add_features_to_layer(layer, new_features):

    logger = get_run_logger()
    logger.info(f"{add_features_to_layer.description}")

    response = layer.edit_features(adds=new_features)

    if response["addResults"]:
        logger.info(f"{len(new_features)} registros adicionados.")
    else:
        raise ValueError(f"Erro ao adicionar registros:", new_features)


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
            result = remove_feature_from_layer(live_layer, df_live_layer)
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
def waze_live_traffic():
    try:
        logger = get_run_logger()
        logger.info(f"{waze_live_traffic.description}")
        waze_json_data = get_api_data_as_json()
        df_api_data = parse_api_data_to_df(waze_json_data)
        gis = create_gis_instance()
        feature_layer = get_layer_byId(
            gis, TRAFFIC_FEATURE_LAYER_ID, TRAFFIC_FEATURE_LAYER_INDEX
        )
        df_fl_live = query_layer(feature_layer)
        if not df_api_data.empty:
            update_feature_layer(df_api_data, df_fl_live, feature_layer)
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")


if __name__ == "__main__":
    waze_live_traffic()
