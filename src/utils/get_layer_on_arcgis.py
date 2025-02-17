from arcgis.gis import GIS


def get_a_layer_index(credentials, layer_index: int):
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
