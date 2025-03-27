import pandas as pd
from prefect import flow

from utils.index import compare_attributes
from sub.hist_alerts import sub_only_in_api, sub_only_in_layer, sub_matching_att


@flow(name="waze-live-alerts")
def waze_live_alerts(df_api, df_live_layer, live_layer):
    try:
        
        #Compara "tx_uuid"'s vindos da API com os "uuid" que já estão na camada live
        compared_data = compare_attributes(
            df_api, "tx_uuid", df_live_layer, "tx_uuid")
        
        
        #Cria uma "Serie" com apenas os itens que estão presentes no dataframe da API
        only_in_API = df_api[df_api['tx_uuid'].isin(
            compared_data["only_in_df"])] if len(compared_data["only_in_df"]) > 0 else pd.DataFrame([])
        
        
        #Cria uma "Serie" com os itens que estão presentes simultaneamente no dataframe da API e na camada live
        matching_attributes = df_api[df_api['tx_uuid'].isin(
            compared_data["matching_attributes"])] if len(compared_data["matching_attributes"]) > 0 else pd.DataFrame([])
        

        #Cria uma "Serie" com os itens que estão presentes apenas na camada live        
        only_in_layer = df_live_layer[df_live_layer['tx_uuid'].isin(
            compared_data["only_in_layer"])] if len(compared_data["only_in_layer"]) > 0 else pd.DataFrame([])
        

        #Quando os itens comparados estiverem presentes apenas no dataframe da API devem ser incluídos na camada live
        # com preenchendo a data de criação no campo startTime                
        if only_in_API is not None and not only_in_API.empty: 
            sub_only_in_api(only_in_API, live_layer)

        
        #Quando os itens comparados estiverem presentes apenas na camada live devem ser excluídos da camada live
        # e incluídos na camada de histórico preenchendo o valor dt_saída com a data referente a exclusão
        if only_in_layer is not None and not only_in_layer.empty:   
            sub_only_in_layer(only_in_layer, live_layer)            
            

        #Quando os itens comparados estiverem presentes em ambos os dataframes o valor do campo "Atualizado" deve ser preenchido com a data atual
        if matching_attributes is not None and not matching_attributes.empty: 
            sub_matching_att(df_live_layer,compared_data, matching_attributes, live_layer)
       
    except Exception as e:
        error_message = str(e)
        raise ValueError(f"Erro durante a execução: {error_message}")

