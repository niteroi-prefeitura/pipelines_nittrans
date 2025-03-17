import os
import requests
from arcgis.gis import GIS
from dotenv import load_dotenv
from arcgis.features import FeatureLayer

load_dotenv()

URL_TO_GENERATE_TOKEN = os.getenv("URL_TO_GENERATE_TOKEN")
URL_GIS_ENTERPRISE = os.getenv("URL_GIS_ENTERPRISE")
PORTAL_USERNAME = os.getenv("PORTAL_USERNAME")
PORTAL_PASSWORD = os.getenv("PORTAL_PASSWORD")
PORTAL_REFERER = os.getenv("URL_GIS_ENTERPRISE")


def generate_portal_token():

    params = {
        "username": PORTAL_USERNAME,
        "password": PORTAL_PASSWORD,
        "referer": PORTAL_REFERER,
        "f": "json",
    }

    response = requests.post(URL_TO_GENERATE_TOKEN, data=params, verify=False)

    if response.status_code == 200:
        token_info = response.json()
        return token_info['token']
    else:
        print(f"Erro ao gerar token: {response.text}")


def get_layer_on_portal(url_layer):
    session = requests.Session()
    session.verify = False 
    url = f"{URL_GIS_ENTERPRISE}/portal"
    token = generate_portal_token()
    GIS(url, token=token, verify_cert=False, session=session)
    feature_layer = FeatureLayer(url_layer)
    return feature_layer

    
def build_new_hist_feature(df):
    new_features = []
    for _, row in df.iterrows():
        att = row.to_dict()
        del att['Lng']
        del att['Lat']
        ##POSSIVEL PROBLEMA
        new_feature = {
            "attributes": att,
            "geometry": {
                "x": row['Lng'],
                "y": row['Lat']
            }
        }
        new_features.append(new_feature)

    return new_features 
    
def create_new_feature(new_features,feature_layer):

    if new_features:
        try:
            response = feature_layer.edit_features(adds=new_features)
            if response['addResults']:
                count_success_true = sum(1 for result in response['addResults'] if result.get('success') == True)
                print(f"Adicionadas {count_success_true} novas features na layer.")
            return True
        except Exception as e:
            error_message = str(e)
            print(f"Erro durante a criação de features na camada: {error_message}")
    else:
        print("Sem features para adicionar.")  
        return False        

    
