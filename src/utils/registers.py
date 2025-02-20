from arcgis.geometry import Polyline


def clear_all_registers_on_agol_layer(agol_layer):

    print("Removendo registros antigos...")
    existing_features = agol_layer.query(
        where="1=1", out_fields="OBJECTID").features
    if existing_features:
        object_ids = [feat.attributes["OBJECTID"]
                      for feat in existing_features]
        agol_layer.delete_features(deletes=",".join(map(str, object_ids)))
        print(f"{len(object_ids)} registros removidos.")
    else:
        print("Nenhum registro antigo encontrado.")


def add_polyline_registers_on_agol_layer(df, attribute_map: dict[str, str], agol_layer):

    update_feature_list = []
    for _, row in df.iterrows():
        paths = [[(point['x'], point['y']) for point in row['line']]]
        polyline = Polyline(
            {"paths": paths, "spatialReference": {"wkid": 4326}})

        attributes = {attr: row[col]
                      for attr, col in attribute_map.items() if col in row}

        feature_template = {
            "attributes": attributes, "geometry": polyline}
        update_feature_list.append(feature_template)

    add_result = agol_layer.edit_features(adds=update_feature_list)

    if "addResults" in add_result:
        added_count = sum(
            1 for res in add_result["addResults"] if res.get("success", False))
        print(f"{added_count} registros adicionados com sucesso.")
    else:
        print("Erro ao adicionar registros:", add_result)
