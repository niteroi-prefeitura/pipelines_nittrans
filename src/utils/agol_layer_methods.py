from arcgis.gis import GIS
import pandas as pd
from prefect import task, get_run_logger

@task(name="Buscar camada Agol", description="Consulta o Id fornecido da camada live no Agol")
def get_layer_agol(credentials, layer_id, layer_index: int):

    logger = get_run_logger()
    logger.info(f"{get_layer_agol.description}")

    try:
        gis = GIS("https://www.arcgis.com",
                  credentials['agol_username'], credentials['agol_password'])
        portal_item = gis.content.get(layer_id)

        if not portal_item:
            raise Exception(
                "Item não encontrado no ArcGIS Portal. Verifique o layer_id.")
        
        logger.info(f'Sucesso ao buscar camada id:{layer_id} e index:{layer_index} no agol')

        return portal_item.layers[layer_index]
    
    except Exception as e:
        raise ValueError(f"Erro ao conectar no ArcGIS: {e}")

@task(name="Criar DF Agol", description="Constrói dataframe baseado nas features da camada live")
def query_layer_agol(layer, attributes="*", where="1=1"):

    logger = get_run_logger()
    logger.info(f"{query_layer_agol.description}")

    existing_features_attributes = []

    try:
        existing_features = layer.query(where, out_fields=[attributes]).features

        for feat in existing_features:
            existing_features_attributes.append(feat.attributes)

        df_existing = pd.DataFrame(existing_features_attributes)

        logger.info('Sucesso ao criar DF live')

        logger.info(f"Amostra dos primeiros registros:\n{df_existing.head(10)}")
        
        return df_existing
    
    except Exception as e:
        raise ValueError(f"Erro ao pegar os atributos da layer: {e}")

@task(name="Remover features agol", description="Modifica camada live removendo features")
def remove_from_agol(layer, df):
    
    logger = get_run_logger()
    logger.info(f"{remove_from_agol.description}")

    if 'tx_uuid' in df.columns:
        uuids_to_delete = df['tx_uuid'].tolist()
    else:
        uuids_to_delete = df['uuid'].tolist()
    
    uuids_formatados = [f"'{uuid}'" for uuid in uuids_to_delete]

    query = ", ".join(uuids_formatados)

    response = layer.delete_features(where=f"uuid IN ({query})")

    if response['deleteResults']:
        logger.info(f"{len(uuids_to_delete)} registros removidos.")
    else:
        logger.info("Falha na exclusão dos itens.")

@task(name="Adicionar features agol", description="Modifica camada live adicionando features")
def add_features_agol(layer, new_features):

    logger = get_run_logger()
    logger.info(f"{add_features_agol.description}") 

    response = layer.edit_features(adds=new_features)

    if response['addResults']:
        logger.info(f"{len(new_features)} registros adicionados.")
    else:
        logger.info("Erro ao adicionar registros:", new_features)

@task(name="Atualizar features agol", description="Modifica camada live atualizando features")
def update_features_agol(layer, features): 

    logger = get_run_logger()
    logger.info(f"{update_features_agol.description}")
    
    response = layer.edit_features(updates=features)
    if response['updateResults']:
        logger.info(f"{len(features)} registros atualizados.")
    else:
        logger.info("Erro ao atualizar registros:", features)