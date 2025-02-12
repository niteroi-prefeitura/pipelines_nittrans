from arcgis.gis import GIS

def get_a_layer_index(AGOL_USERNAME, AGOL_PASSWORD, LAYER_ID, LAYER_INDEX:int):
    try:
        gis = GIS("https://www.arcgis.com", AGOL_USERNAME, AGOL_PASSWORD)
        portal_item = gis.content.get(LAYER_ID)
        if not portal_item:
            raise Exception("Item não encontrado no ArcGIS Portal. Verifique o layer_id.")
        return portal_item.layers[LAYER_INDEX]
    except Exception as e:
        raise ValueError(f"Erro ao conectar no ArcGIS: {e}")