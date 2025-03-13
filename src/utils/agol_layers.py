from arcgis.gis import GIS
import pandas as pd


def get_layer_agol(credentials, layer_id, layer_index: int):
    try:
        gis = GIS("https://www.arcgis.com",
                  credentials['agol_username'], credentials['agol_password'])
        portal_item = gis.content.get(layer_id)
        if not portal_item:
            raise Exception(
                "Item não encontrado no ArcGIS Portal. Verifique o layer_id.")
        return portal_item.layers[layer_index]
    except Exception as e:
        raise ValueError(f"Erro ao conectar no ArcGIS: {e}")


def query_layer_agol(layer, attributes="*", where="1=1"):
    existing_features_attributes = []
    try:
        existing_features = layer.query(where, out_fields=[attributes]).features
        for feat in existing_features:
            existing_features_attributes.append(feat.attributes)
        return pd.DataFrame(existing_features_attributes)
    except Exception as e:
        raise ValueError(f"Erro ao pegar os atributos da layer: {e}")

def remove_from_agol(layer, df):
    uuids_to_delete = df['uuid'].tolist()
    response = layer.delete_features(where=f"uuid IN ({', '.join(map(str, uuids_to_delete))})")
    if response['deleteResults']:
        print(f"{len(uuids_to_delete)} registros removidos.")
    else:
        print("Falha na exclusão dos itens.")

def add_features_agol(layer, new_features):
    response = layer.edit_features(adds=new_features)
    if response['addResults']:
        print(f"{len(new_features)} registros adicionados.")
    else:
        print("Erro ao adicionar registros:", new_features)

def update_features_agol(layer, features): 
    response = layer.edit_features(updates=features)
    if response['updateResults']:
        print(f"{len(features)} registros atualizados.")
    else:
        print("Erro ao atualizar registros:", features)