import requests
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from prefect.variables import Variable
from prefect import task, get_run_logger

gis_variables = Variable.get("gis_portal_variables")

URL_TO_GENERATE_TOKEN = gis_variables["URL_TO_GENERATE_TOKEN"]
URL_GIS_ENTERPRISE = gis_variables["URL_GIS_ENTERPRISE"]

@task(name="Gerar token", description="Gera token para autenticação no portal via api REST")
def generate_portal_token(credentials):

    logger = get_run_logger()
    logger.info(f"{generate_portal_token.description}")

    params = {
        "username": credentials['username'],
        "password": credentials['password'],
        "referer": URL_GIS_ENTERPRISE,
        "f": "json",
    }

    response = requests.post(URL_TO_GENERATE_TOKEN, data=params, verify=False)

    if response.status_code == 200:
        token_info = response.json()
        return token_info['token']
    else:
        logger.info(f"Erro ao gerar token: {response.text}")

@task(name="Buscar camada portal", description="Busca camada portal")
def get_layer_on_portal(url_layer,token):

    logger = get_run_logger()
    logger.info(f"{get_layer_on_portal.description}")

    session = requests.Session()
    session.verify = False 
    url = f"{URL_GIS_ENTERPRISE}/portal"

    GIS(url, token=token, verify_cert=False, session=session)

    feature_layer = FeatureLayer(url_layer)

    return feature_layer

@task(name="Construir objeto hist", description="Constrói features no padrão para camada de histórico")
def build_new_hist_feature(df):

    logger = get_run_logger()
    logger.info(f"{build_new_hist_feature.description}")

    new_features = []

    for _, row in df.iterrows():
        att = row.to_dict()
        del att['Lng']
        del att['Lat']
        new_feature = {
            "attributes": att,
            "geometry": {
                "x": row['Lng'],
                "y": row['Lat']
            }
        }

        new_features.append(new_feature)

    return new_features 

@task(name="Criar novas features", description="Modifica camada hist inserindo novas features")    
def create_new_feature(new_features,feature_layer):
    
    logger = get_run_logger()
    logger.info(f"{create_new_feature.description}")    

    if new_features:
        try:
            response = feature_layer.edit_features(adds=new_features)
            if response['addResults']:
                count_success_true = sum(1 for result in response['addResults'] if result.get('success') == True)
                logger.info(f"Adicionadas {count_success_true} novas features na layer.")
                return True
            else:
                raise Exception("")
                
        except Exception as e:
            error_message = str(e)
            raise ValueError(f"Erro durante a criação de features na camada: {error_message}")
            
    else:
        logger.info("Sem features para adicionar.")  
        return False        

    
