from datetime import timedelta


def filter_by_alert_type(df_alerts, alert_type):
    df_filtered = df_alerts[df_alerts['tx_tipo_alerta'] == alert_type]

    if df_filtered.empty:
        print(
            f'Nenhum dado encontrado após aplicar o filtro do tipo {alert_type}')
        return

    return df_filtered

def filter_by_alert_subtype(df_alerts, alert_subtype):
    df_filtered = df_alerts[df_alerts['tx_subtipo_alerta'] == alert_subtype]

    if df_filtered.empty:
        print(
            f'Nenhum dado encontrado após aplicar o filtro do tipo {alert_subtype}')
        return

    return df_filtered

