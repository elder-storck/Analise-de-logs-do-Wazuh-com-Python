import polars as pl
import pandas as pd
import matplotlib.pyplot as plt

def gerar_grafico_mitre_techniques(
    caminho_arquivo: str = "input/wazuh_august_full.ndjson",
    nome_arquivo_saida: str = "mitre_techniques.png"
):
    """
    Lê um arquivo NDJSON do Wazuh e gera um gráfico horizontal com
    as técnicas MITRE (mitre_techniques) mais frequentes encontradas nos logs.

    Parâmetros:
        caminho_arquivo (str): Caminho para o arquivo NDJSON.
        nome_arquivo_saida (str): Nome do arquivo PNG de saída do gráfico.
    """
    try:
        # 1️⃣ Ler NDJSON
        df = pl.read_ndjson(caminho_arquivo)

        # 2️⃣ Selecionar colunas principais e capturar MITRE de múltiplos caminhos
        df = df.select([
            pl.col("@timestamp"),
            pl.col("rule").struct.field("description").alias("rule.description"),
            pl.col("rule").struct.field("level").alias("rule.level"),
            pl.col("agent").struct.field("name").alias("agent.name"),
            pl.coalesce(
                pl.col("rule").struct.field("mitre_techniques"),
                pl.col("data").struct.field("sca").struct.field("check")
                .struct.field("compliance").struct.field("mitre_techniques"),
                pl.lit(None)
            ).alias("mitre_techniques")
        ])

        # 3️⃣ Converter para Pandas
        df_pd = df.to_pandas()

        # 4️⃣ Normalizar valores MITRE (string → lista)
        def normalizar_mitre(value):
            if value is None:
                return []
            elif isinstance(value, list):
                return value
            elif isinstance(value, str):
                return [value]
            else:
                return []

        df_pd["mitre_techniques"] = df_pd["mitre_techniques"].apply(normalizar_mitre)

        # 5️⃣ Explodir lista (1 linha por técnica)
        df_explodido = df_pd.explode("mitre_techniques")

        # 6️⃣ Filtrar apenas valores válidos
        df_filtrado = df_explodido[
            df_explodido["mitre_techniques"].notna() &
            (df_explodido["mitre_techniques"] != "")
        ]

        if df_filtrado.empty:
            print("⚠️ Nenhum campo 'mitre_techniques' encontrado no arquivo.")
            return None

        # 7️⃣ Contar e ordenar
        contagem = df_filtrado["mitre_techniques"].value_counts().sort_values(ascending=True)

        # 8️⃣ Plotar gráfico horizontal
        plt.figure(figsize=(10, 6))
        contagem.plot(kind="barh", color="royalblue")
        plt.title("Técnicas MITRE mais frequentes nos logs", fontsize=14)
        plt.xlabel("Quantidade de ocorrências")
        plt.ylabel("MITRE Technique ID")
        plt.grid(axis="x", linestyle="--", alpha=0.6)
        plt.tight_layout()
        plt.savefig(nome_arquivo_saida, dpi=200)
        plt.close()

        print(f"✅ Gráfico salvo em: {nome_arquivo_saida}")
        return contagem

    except Exception as e:
        print(f"❌ Erro ao gerar gráfico MITRE: {e}")
        return None
