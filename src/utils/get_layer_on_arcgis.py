from arcgis.gis import GIS


def get_a_layer_object_agol(credentials, layer_index: int):
    try:
        gis = GIS("https://www.arcgis.com",
                  credentials['agol_username'], credentials['agol_password'])
        portal_item = gis.content.get(credentials['layer_id'])
        if not portal_item:
            raise Exception(
                "Item não encontrado no ArcGIS Portal. Verifique o layer_id.")
        return portal_item.layers[layer_index]
    except Exception as e:
        raise ValueError(f"Erro ao conectar no ArcGIS: {e}")


def get_atributes_of_layer_object(layer, attributes):
    existing_features_attributes = []
    try:
        existing_features = layer.query(out_fields=[attributes]).features
        for feat in existing_features:
            existing_features_attributes.append(feat.attributes)
        return existing_features_attributes
    except Exception as e:
        raise ValueError(f"Erro ao pegar os atributos da layer: {e}")
