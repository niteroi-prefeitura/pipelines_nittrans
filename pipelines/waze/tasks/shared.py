from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE
import pandas as pd


@task(
    name="Cria features com data de entrada",
    description="Insere data de entrada e retorna features georreferenciadas",
    cache_policy=NO_CACHE,
)
def create_features(df_api):

    logger = get_run_logger()
    logger.info(f"{create_features.description}")

    try:
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
        count_success_true = sum(
            1 for result in response["addResults"] if result.get("success") == True
        )
        logger.info(f"Adicionadas {count_success_true} novas features na layer.")
    else:
        raise ValueError(f"Erro ao adicionar registros:", new_features)


@task(
    name="Buscar camada por id e index",
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
    name="Remover features",
    description="Modifica camada removendo features",
    cache_policy=NO_CACHE,
)
def remove_features_from_layer(layer, df):

    logger = get_run_logger()
    logger.info(f"{remove_features_from_layer.description}")

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
    name="Criar layer dataframe",
    description="Constrói dataframe baseado nas features da camada live",
    cache_policy=NO_CACHE,
)
def query_layer_dataframe(layer):

    logger = get_run_logger()
    logger.info(f"{query_layer_dataframe.description}")

    attributes = "*"
    where = "1=1"
    existing_features_attributes = []

    try:
        existing_features = layer.query(where, out_fields=[attributes]).features

        for feat in existing_features:
            existing_features_attributes.append(feat.attributes)

        df_existing = pd.DataFrame(existing_features_attributes)

        logger.info("Sucesso ao criar Dataframe de Features em camada")

        return df_existing

    except Exception as e:
        raise ValueError(f"Erro ao pegar os atributos da layer: {e}")


@task(
    name="Atualizar features",
    description="Modifica camada atualizando features",
    cache_policy=NO_CACHE,
)
def update_features(layer, features):

    logger = get_run_logger()
    logger.info(f"{update_features.description}")

    response = layer.edit_features(updates=features)
    if response["updateResults"]:
        logger.info(f"{len(features)} registros atualizados.")
    else:
        logger.info("Erro ao atualizar registros:", features)
