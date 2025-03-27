import datetime
import pytz

def compare_attributes(df, df_attribute, layer, layer_attribute):
    try:
        df_attributes = set(df[df_attribute])        

        if len(layer) > 0:
            layer_attributes = set(layer[layer_attribute])
            matching_attributes = df_attributes.intersection(layer_attributes)
            only_in_df = df_attributes - layer_attributes
            only_in_layer = layer_attributes - df_attributes
            print(f"matching_attributes: {len(matching_attributes)} registros", f"only_in_df: {len(only_in_df)} registros",f"only_in_layer: {len(only_in_layer)} registros")

            return {
                "matching_attributes": matching_attributes,
                "only_in_df": only_in_df,
                "only_in_layer": only_in_layer
            }
        
        else:            
            return {
                "matching_attributes": [],
                "only_in_df": df_attributes,
                "only_in_layer": []
            }
    
    except Exception as e:        
        error_message = str(e)
        raise ValueError(f"Erro durante a comparação: {error_message}")

def create_ms_timestamp(df,col_name):
    brasil_tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.datetime.now(brasil_tz) 
    timestamp_ms = int(now.timestamp()*1000)
    df[col_name] = timestamp_ms