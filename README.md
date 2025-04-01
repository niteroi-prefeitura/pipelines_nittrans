<a id="readme-top">En | Pt-br</a>

<br />
<div align="center">
  <a href="">
      <img src="https://www.ortofotos.niteroi.rj.gov.br/arquivos/Imagens/github_ED/logos/logo%20nittrans%20_%20ED.png" alt="Logo" height="80">
  </a>
  <h3 align="center">Pipelines repository - NitTrans | Repositório de pipelines - NitTrans</h3>
  <p align="center">
    This repository aims to organize and share the pipelines developed for NitTrans - Niterói Transporte e Trânsito.
  </p>
  <p>
    Esse repositório tem como objetivo organizar e compartilhar as pipelines desenvolvidas para a NitTrans - Niterói Transporte e Trânsito.
  </p>
  <p><a href="https://github.com/SIGeo-Niteroi/scripts/issues">Report Bug</a></p>
</div>

<details>
  <summary>Table of contents | Súmario</summary>
  <ol>
    <li>
      <a href="#about-the-repository--sobre-o-repositório">About The Repository | Sobre O Repositório</a>
      <ul>
        <li><a href="#built-with--desenvolvido-com">Built With | Desenvolvido Com</a></li>
      </ul>
    </li>
    <li>
      <a href="#files--arquivos">Files | Arquivos</a>
      <ul>
        <li><a href="#src">src</a>
        <ul>
        <li><a href="#sub">sub</a></li>
        <li><a href="#tasks">tasks</a></li>
        <li><a href="#utils">utils</a></li>
        <li><a href="#waze_hist_traffic">waze_hist_traffic</a></li>
        <li><a href="#waze_pipeline">waze_pipeline</a></li>
        </ul>
        </li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started | Inicializando</a>
      <ul>
        <li><a href="#prerequisites--pré-requisitos">Prerequisites | Pré-requisitos</a></li>
        <li><a href="#installation--instalação">Installation | Instalação</a></li>
      </ul>
    </li>
    <li><a href="#how-prefect-works">What is Prefect and how it works | O que é o Prefect e como ele funciona</a></li>
    <li><a href="#-contributing--contribuindo">Contributing | Contribuindo</a></li>
    <li><a href="#contact--contato">Contact | Contato</a></li>
    <li><a href="#contributors--contribuidores">Contributors | Contribuidores</a></li>
  </ol>
</details>

## About The Repository | Sobre O Repositório

<p>Welcome! This repository, created by the Data Office of the City of Niterói, organizes and shares the pipelines developed for NitTrans (the city's Department of Transportation and Transit).
<br>
Here you will find the scripts used in processing data from the Niterói transit and transportation system, organized into folders according to their purpose.
</p>

<p>Bem vindo(a)! Este repositório, criado pelo Escritório de Dados da Prefeitura de Niterói, organiza e compartilha as pipelines desenvolvidas para a NitTrans (Secretaria de Transporte e Trânsito do município).
<br>
Aqui você encontrará os scripts utilizados no processamento de dados do sistema de trânsito e transporte de Niterói, organizados em pastas conforme sua finalidade.</p>
<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With | Desenvolvido Com

[![Python]][Python-url] [![Prefect]][Prefect-url]![env] ![Arcgis] ![Pandas]

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<h2 id="files--arquivos">Files | Arquivos</h2>

<details id="src">
  <summary>📁 src</summary>
    <details id="sub">
      <summary>📁 sub</summary>
      <p style='margin-left:20px'>In this folder we have the sub-flows that will be used in the pipelines:
      <ul>
        <li><strong>hist_alerts:</strong> Here, we can find flows that perform operations for inserting, updating, and removing features in ArcGIS Online layers, as well as inserting new features into historical traffic alert layers in the ArcGIS Portal. These operations are based on a comparison between the current state of the live traffic layer in Niterói and the data obtained from the Waze API.</li>    
        <li><strong>live_alerts:</strong> Here, we can find the function responsible for updating the live traffic alert layer as well as the historical layers, using the flows present in the "hist-alerts" file.</li>
        <li><strong>live_traffic:</strong> Here, we can find the function responsible for updating the live traffic layer.</li>
      </ul>
      </p>
      <p style='margin-left:20px'>Nesta pasta temos os sub-fluxos que serão usados nas pipelines:
      <ul>
        <li><strong>hist_alerts:</strong> Aqui podemos encontrar fluxos que realizam operações de inserção, atualização e remoção de features em camadas do ArcGis Online, como também a inserção de novas features em camadas de histórico de alertas de trânsito no ArcGis Portal. Tendo como base a comparação entre o estado atual da camada ao vivo de trânsito em Niterói e os dados obtidos da API do Waze.</li>    
        <li><strong>live_alerts:</strong> Aqui podemos encontrar a função responsável por atualizar a camada ao vivo de alertas de trânsito e também as de histórico, usando os fluxos presentes no arquivo "hist-alerts"</li>
        <li><strong>live_traffic:</strong> Aqui podemos encontrar a função responsável por atualizar a camada ao vivo de sobre o tráfego.</li>
      </ul>
      </p>
    </details>
    <details id="tasks">
      <summary>📁 tasks</summary>
      <p style='margin-left:20px'>In this folder we have the functions that will handle the data request to the external API and transform it into a manipulable dataframe:
      <ul>
        <li><strong>get_api_data:</strong> Here we can find a function that makes a request to an API and returns the response as JSON.</li>    
        <li><strong>parse_to_dataframe:</strong> Here we can find flows responsible for receiving data from the API and transforming it into a dataframe with the specific pattern for each ArcGIS layer that we need.</li>
      </ul>
      </p>
      <p style='margin-left:20px'>Nesta pasta temos as funções que vão lidar com a requisição dos dados à API externa e tranformá-los em um dataframe manipulável:
      <ul>
        <li><strong>get_api_data:</strong> Aqui podemos encontrar uma função que faz uma requisição a uma api e retorna a resposta como JSON.</li>    
        <li><strong>parse_to_dataframe:</strong> Aqui podemos encontrar fluxos responsáveis por receber os dados da api e tranformá-los em um dataframe com o padrão específico para cada camada do ArcGis que precisamos.</li>
      </ul>
      </p>
    </details>
    <details id="utils">
      <summary>📁 utils</summary>
      <p style='margin-left:20px'>In this folder we have the functions that will handle the data request to the external API and transform it into a manipulable dataframe:
      <ul>
        <li><strong>agol_layer_methods:</strong>File that contains methods for reading, inserting, updating, and removing features in ArcGIS Online layers.</li>    
        <li><strong>index:</strong>In this file we have auxiliary functions that are used to manipulate and transform DataFrames and type columns, features for comparing attributes and creating timestamps.</li>
        <li><strong>portal_layer_methods:</strong>File containing methods for authentication and manipulation of layers in AcrGis Portal.</li>
      </ul>
      </p>
      <p style='margin-left:20px'>Nesta pasta temos métodos que são chamados sem ordem especifica, sendo reutilizados em vários contextos:
      <ul>
        <li><strong>agol_layer_methods:</strong>Arquivo que contém métodos  de leitura, inserção, atualização e remoção de features em camadas do ArcGIS Online.</li>    
        <li><strong>index:</strong>Neste arquivo temos funções auxiliares que servem para manipulação e transformação de DataFrames e tipagem de colunas, funcionalidades para comparação de atributos e criação de timestamp</li>    
        <li><strong>portal_layer_methods:</strong>Arquivo que contém métodos para autenticação e manipulação de camadas no AcrGis Portal.</li>
      </ul>
      </p>
    </details>
    <details id="waze_his_traffic">
      <summary>📄 waze_his_traffic</summary>
      <p style='margin-left:20px'>
        Pipeline that receives Traffic data from the Waze API and updates the Traffic History layer in ArcGis Portal.
      </p>
      <p style='margin-left:20px'>
        Pipeline que recebe os dados de Tráfego da API do Waze e atualiza a camada de Histórico de Tráfego no ArcGis Portal.
      </p>
    </details>
    <details id="waze_pipeline">
      <summary>📄 waze_pipeline</summary>
      <p style='margin-left:20px'>
        Pipeline that receives Traffic data from the Waze API, updates the Live Traffic and Alerts layers in ArcGis Online, as well as the historical alerts in the ArcGis Portal layers.
      </p>
      <p style='margin-left:20px'>
        Pipeline que recebe os dados de Tráfego da API do Waze, atualiza as camadas Ao vivo de Trânsito e de Alertas no ArcGis Online, como também as de histórico de alertas nas camdas do ArcGis Portal.
      </p>
    </details>
</details>
<!-- <details>
  <summary id="env.example">📄 .env.example</summary>
      <p style='margin-left:20px'>
        File containing the environment variables necessary for the project.
      </p>
      <p style='margin-left:20px'>
        Arquivo que com as variáveis de âmbiente nesessárias para o projeto.
      </p>
</details> -->




<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- GETTING STARTED -->
<h2 id="getting-started">Getting Started</h2>

<p>First, install the dependencies needed to run this project</p>

<p>Primeiro, instale as dependências necessárias para rodar o projeto</p>

<h3>Prerequisites | Pré-requisitos</h3>

- Python -> https://www.python.org/
- Arcpy -> https://pro.arcgis.com/en/pro-app/latest/arcpy/get-started/what-is-arcpy-.htm
- Arcgis.gis -> https://developers.arcgis.com/python/

<h3 id="installation--instalação">Installation | Instalação</h3>

1. Clone the repo | Clone o repositório
   ```sh
   git clone https://github.com/...
   ```

2. Install Libraries | Instale as bibliotecas

3. Create a .env local file based on the .env.example file | Crie um arquivo local .env baseado no arquivo .env.example
   *When necessary | quando necessário* 

4. Start the application | Rode o script
    ```sh
    ptyhon nome-do-script.py
   ```
<p>❗ NOTE: These pipelines are being executed by the Prefect Workflows manager, which saves the environment variables used. To use them locally, you will need to replace the variable values ​​as needed.</p>

<p>❗ OBS.: Essas pipelines estão sendo executadas pelo gerenciador de Workflows Prefect, que guarda as variáveis de âmbiente usadas. Para usar localmente será necessário que você substitua os valores das variáveis de acordo com a necessidade.</p>


<p align="right">(<a href="#readme-top">back to top</a>)</p>

<h2 id="how-prefect-works">What is Prefect and how it works | O que é o Prefect e como ele funciona</h2>

<p>
  O Prefect é uma ferramenta de orquestração de workflows que facilita a execução, o gerenciamento e o monitoramento de pipelines de dados e automações. Ele permite definir fluxos de trabalho como código Python e oferece recursos como agendamento, controle de dependências, logging e monitoramento.
</p>
<h3>Como ele executa pipelines do Github?</h3>
<ol>
  <li><strong>Código salvo no Github:</strong>
  <br>
  • O código do fluxo (flow) está no repositório do GitHub.
  </li>

  <li><strong>Definição de um Deployment do Prefect</strong>
  <br>
  • Define quando e como o fluxo será executado, indicamos no proprio código através do "@flow":
  <br>
  <code>
    @flow(name="waze-live-hist",log_prints=True)<br>
    def waze_live():
    try:
  </code>
  <br>
  • Ele pode ser configurado para rodar periodicamente (agendado) ou ser acionado manualmente.
  </li>

  <li><strong>Conexão do Prefect com o Repositório</strong>
  <br>
  • O Prefect pode buscar o código no GitHub automaticamente. Para isso, configura-se um Storage Block (GitHub block) no Prefect Cloud ou Prefect Server.
  </li>
  <li><strong>Execução do Pipeline</strong>
  <br>
  • Quando o fluxo é acionado (manualmente ou via agendamento), o Prefect baixa o código do GitHub e o executa na infraestrutura configurada.
  </li>
</ol>

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<h2>🤝 Contributing | Contribuindo</h2> 
Contributions are **greatly appreciated**! | Contribuições são **sempre bem vindas**!

If you have a suggestion that would make this project better, please fork the repo and create a pull request. You can also open an issue with the tag "enhancement".
<p>Se você possuir alguma sugestão que possa tornar esse projeto melhor, por favor fork esse repositório e crie um pull request. Você pode também abrir um issue com a tag "enhancement".</p>

1. Fork the Project | Fork o Projeto
2. Create your Feature Branch | Crie sua  Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes | Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch | Push para sua Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request | Abra um Pull Request

Thanks! Obrigado! 😄

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<h2>Contact | Contato</h2>

Sistema de Gestão de Geoinformação - [Portal SIGeo](https://www.sigeo.niteroi.rj.gov.br/) - atendimento@sigeo.niteroi.rj.gov.br

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<h2>Contributors | Contribuidores</h2>

<a href="https://github.com/niteroi-prefeitura/pipelines_nittrans/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=niteroi-prefeitura/pipelines_nittrans" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

<p align="right">(<a href="#readme-top">back to top</a>)</p>


[Python]: https://img.shields.io/badge/Python-14354C?style=for-the-badge&logo=python&logoColor=white
[Pandas]: https://img.shields.io/badge/Pandas-4007a2?style=for-the-badge&logo=pandas&logoColor=white
[Prefect]: https://img.shields.io/badge/Prefect-0c1b1f?style=for-the-badge&logo=prefect&logoColor=white
[Arcgis]: https://img.shields.io/badge/ArcGIS-2C7AC3.svg?style=for-the-badge&logo=ArcGIS&logoColor=white
[env]: https://img.shields.io/badge/.ENV-ECD53F.svg?style=for-the-badge&logo=dotenv&logoColor=black
[Python-url]: https://www.python.org/
[Prefect-url]: https://www.prefect.io/