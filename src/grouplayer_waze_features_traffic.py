import os
from dotenv import load_dotenv
from process_data import process_data
from get_api_data import get_api_data_as_json
from send_email import send_email_error
from registers import clear_old_registers, add_registers
from get_layer_on_arcgis import get_a_layer_index

load_dotenv()

AGOL_USERNAME = os.getenv("AGOL_USERNAME")
AGOL_PASSWORD = os.getenv("AGOL_PASSWORD")
LAYER_ID = os.getenv("LAYER_ID")

def update_layers_on_arcgis(traffic_layer, df):
    errors = []

    registers_removed = clear_old_registers(traffic_layer)
    if isinstance(registers_removed, str):
        errors.append(registers_removed)
    
    registers_added = add_registers(df, traffic_layer)
    if isinstance(registers_added, str):
        errors.append(registers_added)

    return errors

def main():
    api_url = "https://www.waze.com/row-partnerhub-api/partners/18863635230/waze-feeds/feef83ff-1b4f-4d93-9d7a-dd3ea11304d3?format=1"
    errors = []
    
    try:
        json_response = get_api_data_as_json(api_url)

        df = process_data(json_response)

        traffic_layer = get_a_layer_index(AGOL_USERNAME, AGOL_PASSWORD, LAYER_ID, 2)

        errors += update_layers_on_arcgis(traffic_layer, df)
        
    except Exception as e:
        errors.append(str(e))
    
    finally:
        if errors:
            send_email_error(errors)
            
if __name__ == "__main__":
    main()