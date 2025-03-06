import os
from arcgis.gis import GIS
from dotenv import load_dotenv
from arcgis.features import FeatureLayer

load_dotenv()

URL_GIS_ENTERPRISE = os.getenv("URL_GIS_ENTERPRISE")


def update_point_layers_on_portal(df, credentials, token):
    print(f"Colunas do DataFrame: {df.columns.tolist()}")

    existing_uuids = {
        feat.attributes['uuid']: feat.attributes['OBJECTID'] for feat in existing_features}
    print(f"Valor de existing_uuids: {existing_uuids}")
    # df_uuids = set(df['uuid'])

    # url = f"{URL_GIS_ENTERPRISE}/portal"

    # GIS(url, token=token)

    # layer_name = credentials["layer_name"]
    # item = credentials["item"]
    # print(f"Valor de layer_name: {layer_name}/nValor de item: {item}")
    # Acessando a feature layer

    # feature_layer = f"{URL_GIS_ENTERPRISE}/server/rest/services/Hosted/{layer_name}/FeatureServer/{item}"

    # feature_layer = FeatureLayer(
    #     f"{url_gis_enterprise}/server/rest/services/{layer_name}/FeatureServer/{item}")

    # uuids_to_add = df_uuids - set(existing_uuids.keys())

    # Iterando no df DataFrame e fazendo append das features na layer
    # if uuids_to_add:
    #     new_features = []
    #     for _, row in df[df['uuid'].isin(uuids_to_add)].iterrows():
    #         new_feature = {
    #             "attributes": row.to_dict(),
    #             "geometry": {
    #                 "x": row['db_longitude'],
    #                 "y": row['db_latitude']
    #             }
    #         }
    #         new_features.append(new_feature)

    #     # Add new features to the layer
    #     if new_features:
    #         feature_layer.edit_features(adds=new_features)
    #         print(f"Adicionadas {len(new_features)} novas features na layer.")
    #     else:
    #         print("Sem features para adicionar.")
