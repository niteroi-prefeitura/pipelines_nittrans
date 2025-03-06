def compare_attributes(df, df_attribute, layer, layer_attribute):
    df_attributes = set(df[df_attribute])
    layer_attributes = {obj[layer_attribute] for obj in layer}

    print(f'UUIDs no DataFrame: {df_attributes}')
    print(f'UUIDs na camada: {layer_attributes}')

    matching_attributes = df_attributes.intersection(layer_attributes)
    only_in_df = df_attributes - layer_attributes
    only_in_layer = layer_attributes - df_attributes

    return {
        "matching_attributes": matching_attributes,
        "only_in_df": only_in_df,
        "only_in_layer": only_in_layer
    }
