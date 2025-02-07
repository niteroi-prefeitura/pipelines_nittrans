from arcgis.geometry import Polyline

def limpar_registros_antigos(traffic_agol):

    print("Removendo registros antigos...")
    existing_features = traffic_agol.query(where="1=1", out_fields="OBJECTID").features
    if existing_features:
        object_ids = [feat.attributes["OBJECTID"] for feat in existing_features]
        traffic_agol.delete_features(deletes=",".join(map(str, object_ids)))
        print(f"{len(object_ids)} registros removidos.")
    else:
        print("Nenhum registro antigo encontrado.")

def adicionar_registros(df, traffic_agol):

    update_feature_list = []
    for _, row in df.iterrows():
        paths = [[(point['x'], point['y']) for point in row['line']]]
        polyline = Polyline({"paths": paths, "spatialReference": {"wkid": 4326}})

        attributes = {
            'Pais': row['Pais'],
            'uuid': row['uuid'],
            'street': row['street'],
            'Final': str(row['endNode']),
            'Tipo_da_rua': row['Tipo_da_rua'],
            'Comprimento': row['length'],
            'Vel_km_h': row['Vel_km_h'],
            'Cidade': row['Cidade'],
            'Level_': row['level'],
            'delay': row['delay'],
            'speed': row['speed'],
            'turnType': row['turnType'],
            'Data': row['Data']
        }
        feature_template = {"attributes": attributes, "geometry": polyline}
        update_feature_list.append(feature_template)

    add_result = traffic_agol.edit_features(adds=update_feature_list)
    
    if "addResults" in add_result:
        added_count = sum(1 for res in add_result["addResults"] if res.get("success", False))
        print(f"{added_count} registros adicionados com sucesso.")
    else:
        print("Erro ao adicionar registros:", add_result)