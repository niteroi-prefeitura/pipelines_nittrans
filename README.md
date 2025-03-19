# Scripts para aplicações que usam o WAZE

## live_traffic_update
O script recebe atualizações da API do Waze, filtrando os consgestionamentos (JAMs) e processa os dados (pegando apenas os dados de Niterói) para atualizar a camada de Live Traffic no ArcGis Online.

## hist_accident_update
O script recebe atualizações da API do Waze, filtrando apenas informações sobre alertas do tipo acidente, compara com os dados que existem na camada LIVE de acidentes para identificar os novos alertas recebidos pela consulta à API, as novas informações são usadas para popular a camada de histórico de acidentes no ArcGis Portal.
