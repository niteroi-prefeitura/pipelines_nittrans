from prefect import task

@task(name="Comparar Atributos", description="Compara o dataframe vindo da api e da camada live com base no atributo")
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
            print(f"matching_attributes: {len(matching_attributes)} registros", f"only_in_df: {len(only_in_df)} registros",f"only_in_layer: {len(only_in_layer)} registros")
            return {
                "matching_attributes": [],
                "only_in_df": df_attributes,
                "only_in_layer": []
            }
    
    except Exception as e:        
        error_message = str(e)
        raise ValueError(f"Erro durante a comparação: {error_message}")
