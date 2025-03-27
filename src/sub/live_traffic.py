from utils.agol_layer_methods import remove_from_agol, add_features_agol
from arcgis.geometry import Polyline
from prefect import flow


@flow(name="Fluxo live traffic", description="Atualiza camada de trafego live")
def waze_live_traffic(df_api, df_live_layer, live_layer):
    
    try:        

        features_to_add = []

        for _, row in df_api.iterrows():

            paths = [[(point['x'], point['y']) for point in row['line']]]
            polyline = Polyline({"paths": paths, "spatialReference": {"wkid": 4326}})

            att = row.to_dict()
            del att['line']

            feature_template = {"attributes": att, "geometry": polyline}
            features_to_add.append(feature_template)

        if not df_live_layer.empty:
            result = remove_from_agol(live_layer,df_live_layer)
            if result == True:   
                add_features_agol(live_layer,features_to_add) 
        else:
            add_features_agol(live_layer,features_to_add)

    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro ao preparar dados de trafego : {error_message}")
