from arcgis.gis import GIS
import pandas as pd
from prefect import task

@task(name="Buscar camada Agol", description="Consulta o ID da camada live no Agol")
def get_layer_agol(credentials, layer_id, layer_index: int):
    try:
        gis = GIS("https://www.arcgis.com",
                  credentials['agol_username'], credentials['agol_password'])
        portal_item = gis.content.get(layer_id)
        if not portal_item:
            raise Exception(
                "Item não encontrado no ArcGIS Portal. Verifique o layer_id.")
        print('Sucesso ao buscar camada agol')
        return portal_item.layers[layer_index]
    except Exception as e:
        raise ValueError(f"Erro ao conectar no ArcGIS: {e}")

@task(name="Criar DF Agol", description="Constrói dataframe baseado nas features da camada live")
def query_layer_agol(layer, attributes="*", where="1=1"):
    existing_features_attributes = []
    try:
        existing_features = layer.query(where, out_fields=[attributes]).features
        for feat in existing_features:
            existing_features_attributes.append(feat.attributes)
        print('Sucesso ao criar DF live')
        return pd.DataFrame(existing_features_attributes)
    except Exception as e:
        raise ValueError(f"Erro ao pegar os atributos da layer: {e}")

@task(name="Remover features agol", description="Modifica camada live removendo features")
def remove_from_agol(layer, df):
    uuids_to_delete = df['uuid'].tolist()
    uuids_formatados = [f"'{uuid}'" for uuid in uuids_to_delete]
    query = ", ".join(uuids_formatados)
    response = layer.delete_features(where=f"uuid IN ({query})")
    if response['deleteResults']:
        print(f"{len(uuids_to_delete)} registros removidos.")
    else:
        print("Falha na exclusão dos itens.")

@task(name="Adicionar features agol", description="Modifica camada live adicionando features")
def add_features_agol(layer, new_features):
    response = layer.edit_features(adds=new_features)
    if response['addResults']:
        print(f"{len(new_features)} registros adicionados.")
    else:
        print("Erro ao adicionar registros:", new_features)

@task(name="Atualizar features agol", description="Modifica camada live atualizando features")
def update_features_agol(layer, features): 
    response = layer.edit_features(updates=features)
    if response['updateResults']:
        print(f"{len(features)} registros atualizados.")
    else:
        print("Erro ao atualizar registros:", features)