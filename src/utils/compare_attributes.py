#df_api, "tx_uuid", df_live_layer, "uuid"
def compare_attributes(df, df_attribute, layer, layer_attribute):
    try:
        df_attributes = set(df[df_attribute])
        layer_attributes = set(layer[layer_attribute])

        matching_attributes = df_attributes.intersection(layer_attributes)
        only_in_df = df_attributes - layer_attributes
        only_in_layer = layer_attributes - df_attributes

        return {
            "matching_attributes": matching_attributes,
            "only_in_df": only_in_df,
            "only_in_layer": only_in_layer
        }
    
    except Exception as e:
        error_message = str(e)
        print(f"Erro durante a comparação: {error_message}")
