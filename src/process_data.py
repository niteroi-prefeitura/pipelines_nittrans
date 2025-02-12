from datetime import datetime
import pandas as pd

def process_data(data):

    df = pd.DataFrame(data['jams'])
    
    if df.empty:
        raise ValueError(f"Erro no processamento de dados: {df}")

    df['Pais'] = "BR"
    df['Cidade'] = "NITEROI"
    df['Vel_km_h'] = df['speedKMH']
    df['Tipo_da_rua'] = df['roadType']

    now = datetime.now()
    df['Data'] = now.strftime(format='%d/%m/%Y %H:%M')
    df['Data'] = pd.to_datetime(df['Data'], dayfirst=True)
    df['Data'] = df['Data'].apply(lambda x: int(x.timestamp() * 1000))

    return df