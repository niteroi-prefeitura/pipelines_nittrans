def update_point_layers_on_portal(df, url_gis_enterprise, token, live_layer):
    existing_features = live_layer.query(out_fields=["uuid"]).features
    existing_uuids = {
        feat.attributes['uuid']: feat.attributes['OBJECTID'] for feat in existing_features}
    df_uuids = set(df['uuid'])
    # Função para atualizar os valores da camada existente
    url = f"{url_gis_enterprise}/portal"

    # Conectar ao portal com o token
    GIS(url, token=token)

    layer_name = "NITTRANS_WAZE_P_HIST_ACIDENTE"
    item = 0
    # Acessando a feature layer
    feature_layer = FeatureLayer(
        f"{url_gis_enterprise}/server/rest/services/{layer_name}/FeatureServer/{item}")

    uuids_to_add = df_uuids - set(existing_uuids.keys())

    # Iterando no df DataFrame e fazendo append das features na layer
    if uuids_to_add:
        new_features = []
        for _, row in df[df['uuid'].isin(uuids_to_add)].iterrows():
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
