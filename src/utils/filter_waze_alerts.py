from datetime import timedelta


def filter_waze_alerts_by_alert_type(df_alerts, alert_type):
    df_filtered = df_alerts[df_alerts['tx_tipo_alerta'] == alert_type]

    if df_filtered.empty:
        print(
            f'Nenhum dado encontrado após aplicar o filtro do tipo {alert_type}')
        return

    mapeamento_tipo_via = {
        1: 'Rua',
        2: 'Rua principal',
        3: 'Via expressa',
        4: 'Rampa de acesso',
        5: 'Trilha',
        6: 'Principal',
        7: 'Secundária',
        8: 'Trilha 4X4',
        14: 'Trilha 4X4',
        15: 'Travessia de balsa',
        9: 'Passarela',
        10: 'Passagem para pedestres',
        11: 'Saída',
        16: 'Escadaria',
        17: 'Via particular',
        18: 'Ferrovia',
        19: 'Pista de pouso, decolagem e taxiamento',
        20: 'Via de estacionamento',
        21: 'Via de serviço'
    }

    # Substituir os valores em 'tx_tipo_via' de acordo com o mapeamento criado
    df_alerts['tx_tipo_via'] = df_alerts['tx_tipo_via'].map(
        mapeamento_tipo_via)

    # Adicionar 3 horas na coluna 'dt_data_hora'
    df_filtered['dt_data_hora'] = df_filtered['dt_data_hora'] + \
        timedelta(hours=3)

    return df_filtered
