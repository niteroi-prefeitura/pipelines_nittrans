import os
from arcgis.gis import GIS
from dotenv import load_dotenv
from arcgis.features import FeatureLayer

load_dotenv()

URL_GIS_ENTERPRISE = os.getenv("URL_GIS_ENTERPRISE")


def get_layers_on_portal(layer_name,item, token):
    url = f"{URL_GIS_ENTERPRISE}/portal"

    GIS(url, token=token)

    feature_layer = FeatureLayer(
        f"{URL_GIS_ENTERPRISE}/server/rest/services/{layer_name}/FeatureServer/{item}")

    return feature_layer

    
def create_new_feature(df,feature_layer):
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

    if new_features:
        feature_layer.edit_features(adds=new_features)
        print(f"Adicionadas {len(new_features)} novas features na layer.")
    else:
        print("Sem features para adicionar.")  
    

    
