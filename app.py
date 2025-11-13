from tokenize import String
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from pandas import DataFrame
from typing import Optional
import altair as alt
from utils import transformar_tempo, ler_logs_ndjson_polars, criar_grafico_barra_vertical, criar_grafico_barra_horizontal
from graficosPorRegras import analisar_mensagens_generic_events
from mitre import gerar_grafico_mitre_techniques
import seaborn as sns
import os

# Caminho do arquivo
path_august     = "input/wazuh_august_full.ndjson"
path_september  = "input/wazuh_september_full.ndjson"
path_october    = "input/wazuh_october_full.ndjson"

# Lê e achata o NDJSON
df_august = ler_logs_ndjson_polars(path_august)
df_september = ler_logs_ndjson_polars(path_september)
df_october = ler_logs_ndjson_polars(path_october)

# Concatenar todos os meses
df_all = pl.concat([df_august, df_september, df_october], how="vertical")


# ====================quantidade de logs ==================================================================================================================
quantidade_de_logs_totais : int = len(df_all)
print(f"Quantidade de logs totais = {quantidade_de_logs_totais}\n")


# ====================quantidade de logs por level ==================================================================================================================
quantidade_de_logs_por_level = df_all["rule.level"].value_counts().sort("rule.level")
print(quantidade_de_logs_por_level)

df_all_pd = df_all.to_pandas()

# Cria gráficos por level
criar_grafico_barra_vertical(
    df=df_all_pd,
    coluna="rule.level",
    titulo="Distribuição de Logs por Nível",
    nome_arquivo="logs_por_level.png"
)

# ====================quantidade de logs por mes ==================================================================================================================

df_all_pd = transformar_tempo(df_all_pd)

criar_grafico_barra_vertical(
    df=df_all_pd,
    coluna="mes",
    titulo="Distribuição de Logs por Mês",
    nome_arquivo="logs_por_mes.png"
)

# ====================quantidade de agentes monitorados ==================================================================================================================

criar_grafico_barra_horizontal(
    df=df_all_pd,
    coluna="agent.name",
    titulo="Quantidade de Logs por Agentes",
    nome_arquivo="logs_por_agentes.png"
)

# ==================== regras acionadas ==================================================================================================================
criar_grafico_barra_horizontal(
    df=df_all_pd,
    coluna="rule.description",
    titulo="Quantidade de Regras Acionadas",
    nome_arquivo="logs_por_regras.png"
)

# ==================== regras acionadas - sem o generic event==================================================================================================================

# Filtrar o DataFrame para remover a regra "Generic event detected from network device."
df_filtrado = df_all_pd[df_all_pd["rule.description"] != "Generic event detected from network device."]

criar_grafico_barra_horizontal(
    df=df_filtrado,
    coluna="rule.description",
    titulo="Quantidade de Regras Acionadas (Sem Regra Genérica)",
    nome_arquivo="logs_por_regras_sem_generica.png"
)


# ==================== Mensagens dentro do generic event mais acionadas==================================================================================================================

analisar_mensagens_generic_events(df_all_pd)


# ==================== Mensagens dos dispositivos de rede==================================================================================================================

# Sem dispositivos de rede
df_sem_dispositivo_de_rede = df_all_pd[
    ~df_all_pd["location"].fillna("").str.startswith("200")
]
criar_grafico_barra_horizontal(
    df=df_sem_dispositivo_de_rede,
    coluna="rule.description",
    titulo="Quantidade de Regras Acionadas Endpoints",
    nome_arquivo="logs_por_regras_nos_endpoints.png"

)

# Com os dispositivos de rede
df_com_dispositivo_de_rede = df_all_pd[
    df_all_pd["location"].fillna("").str.startswith("200")
]
criar_grafico_barra_horizontal(
    df=df_com_dispositivo_de_rede,
    coluna="rule.description",
    titulo="Quantidade de Regras Acionadas nos Dispositivos de Rede",
    nome_arquivo="logs_por_regras_nos_roteadores.png"
)














# ==================== regras acionadas - sem o generic event==================================================================================================================

# contagem = gerar_grafico_mitre_techniques("logs.ndjson")
# print(contagem)

# Filtrar linhas que têm valores MITRE válidos (não vazios)
# Filtrar linhas onde mitre_techniques não é vazio
# df_filtrado_pd = df_all_pd[
#     df_all_pd["mitre_techniques"].apply(lambda x: len(x) > 0 if isinstance(x, list) else False)
# ]
# print(df_filtrado_pd.shape)
# print(df_filtrado_pd["mitre_techniques"].head(10))
# criar_grafico_barra_horizontal(
#     df=df_all_pd,
#     coluna="mitre_techniques",
#     titulo="Quantidade de rule.mitre_techniques",
#     nome_arquivo="logs_por_rule_mitre.png"
# )
# criar_grafico_barra_horizontal(
#     df=df_all_pd,
#     coluna="data.mitre_techniques",
#     titulo="Quantidade de data.mitre_techniques",
#     nome_arquivo="logs_por_data_mitre_techniques.png"
# )