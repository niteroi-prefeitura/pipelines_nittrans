import os
from arcgis.gis import GIS
from dotenv import load_dotenv
from arcgis.features import FeatureLayer

load_dotenv()

URL_GIS_ENTERPRISE = os.getenv("URL_GIS_ENTERPRISE")


def update_point_layers_on_portal(df, credentials, token):

    url = f"{URL_GIS_ENTERPRISE}/portal"

    GIS(url, token=token)

    layer_name = credentials["layer_name"]
    item = credentials["item"]

    # Acessando a feature layer

    feature_layer = FeatureLayer(
        f"{URL_GIS_ENTERPRISE}/server/rest/services/{layer_name}/FeatureServer/{item}")

    # Iterando no df DataFrame e fazendo append das features na layer
    if not df.empty:
        new_features = []
        for _, row in df.iterrows():
            new_feature = {
                "attributes": row.to_dict(),
                "geometry": {
                    "x": row['db_longitude'],
                    "y": row['db_latitude']
                }
            }
            new_features.append(new_feature)

        # Add new features to the layer
        if new_features:
            feature_layer.edit_features(adds=new_features)
            print(f"Adicionadas {len(new_features)} novas features na layer.")
        else:
            print("Sem features para adicionar.")
    else:
        return
