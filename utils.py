import pandas as pd
import polars as pl
import os
import matplotlib.pyplot as plt
import seaborn as sns

def ler_logs_ndjson_polars(path: str) -> pl.DataFrame:
    df = pl.read_ndjson(path)
    df = df.select([
        pl.col("@timestamp"),
        pl.col("rule").struct.field("level").alias("rule.level"),
        pl.col("rule").struct.field("description").alias("rule.description"),
        pl.col("agent").struct.field("name").alias("agent.name"),
        # Campos que contêm as mensagens detalhadas
        pl.col("full_log").alias("full_log"),
        pl.col("location").alias("location"),
        # Campos MITRE com tratamento individual
        # pl.coalesce(pl.col("rule").struct.field("mitre_techniques"), pl.lit("N/A")).alias("mitre_techniques"),
        # pl.coalesce(pl.col("rule").struct.field("mitre_tactics"), pl.lit("N/A")).alias("mitre_tactics"),
        # pl.coalesce(pl.col("rule").struct.field("mitre_mitigations"), pl.lit("N/A")).alias("mitre_mitigations")
        # # # Campos MITRE do data.sca.check.compliance
        # pl.col("data").struct.field("sca").struct.field("check").struct.field("compliance").struct.field("mitre_techniques").alias("data.mitre_techniques"),
        # pl.col("data").struct.field("sca").struct.field("check").struct.field("compliance").struct.field("mitre_tactics").alias("data.mitre_tactics"),
        # pl.col("data").struct.field("sca").struct.field("check").struct.field("compliance").struct.field("mitre_mitigations").alias("data.mitre_mitigations")
      
    ])
    return df







def transformar_tempo(df, coluna_timestamp="@timestamp"):
    """
    Converte o campo de timestamp para datetime e cria colunas:
    - date: data completa (tipo date)
    - dia: string no formato 'YYYY-MM-DD'
    - mes: mês (YYYY-MM)
    - semana_iso: número da semana ISO
    """
    if coluna_timestamp not in df.columns:
        if "timestamp" in df.columns:
            coluna_timestamp = "timestamp"
        else:
            raise ValueError("Coluna de timestamp não encontrada.")

    # Converte para datetime (ignora erros)
    df[coluna_timestamp] = pd.to_datetime(df[coluna_timestamp], errors="coerce")

    # Cria colunas derivadas de tempo
    df["date"] = df[coluna_timestamp].dt.date
    df["dia"] = df[coluna_timestamp].dt.strftime("%Y-%m-%d")
    df["mes"] = df[coluna_timestamp].dt.to_period("M").astype(str)
    df["semana_iso"] = df[coluna_timestamp].dt.isocalendar().week

    return df



def criar_grafico_barra_vertical(
    df: pd.DataFrame,
    coluna: str,
    titulo: str,
    nome_arquivo: str,
    top_n: int = 15,
    paleta: str = "Blues_r"
):
    """
    Gera um gráfico de barras verticais com estilo acadêmico (TCC) e salva em PNG.
    Versão com dados pré-agregados (mesmo padrão da horizontal).

    Parâmetros:
    -----------
    df : pd.DataFrame
        DataFrame contendo os dados.
    coluna : str
        Nome da coluna para plotar no eixo X.
    titulo : str
        Título do gráfico.
    nome_arquivo : str
        Nome do arquivo de saída (sem caminho).
    top_n : int, opcional
        Número de top categorias para mostrar (padrão: 15).
    paleta : str, opcional
        Paleta de cores (padrão: 'Blues_r').
    """

    # Criar diretório de saída
    os.makedirs("resultados", exist_ok=True)

    # Configuração estética (padrão acadêmico)
    sns.set_theme(style="whitegrid", font_scale=1.2)
    
    # Calcular as top N categorias mais frequentes
    contagem = df[coluna].value_counts().head(top_n)
    
    # Se não houver dados suficientes, usar todos disponíveis
    if len(contagem) == 0:
        print(f"Nenhum dado encontrado na coluna: {coluna}")
        return
    
    # Preparar dados para o gráfico
    categorias = contagem.index.tolist()
    valores = contagem.values.tolist()
    
    # Criar figura
    plt.figure(figsize=(12, 8))
    
    # Criar gráfico de barras VERTICAIS
    ax = sns.barplot(
        x=categorias,
        y=valores,
        palette=paleta
    )

    # Adicionar rótulos acima das barras
    for i, (p, valor) in enumerate(zip(ax.patches, valores)):
        ax.text(
            p.get_x() + p.get_width() / 2,
            p.get_height() + p.get_height() * 0.01,
            f"{int(valor):,}",
            ha='center', 
            va='bottom', 
            fontsize=10,
            weight='bold'
        )

    # Títulos e legendas claros
    plt.title(titulo, fontsize=16, weight="bold", pad=20)
    plt.xlabel(coluna.replace("_", " ").title(), fontsize=12)
    plt.ylabel("Quantidade", fontsize=12)
    
    # Rotacionar labels do eixo X se necessário (para textos longos)
    if any(len(str(cat)) > 15 for cat in categorias):
        plt.xticks(rotation=45, ha='right')
    
    # Ajustar limites do eixo Y para caber os rótulos
    y_max = max(valores) * 1.15
    plt.ylim(0, y_max)
    
    # Grid mais suave
    plt.grid(axis='y', alpha=0.3)
    plt.grid(axis='x', alpha=0.1)
    
    # Layout mais ajustado
    plt.tight_layout()

    # Caminho completo do arquivo de saída
    saida_png = os.path.join("resultados", nome_arquivo)

    # Salvar imagem com qualidade TCC (300 DPI)
    plt.savefig(saida_png, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Gráfico vertical em: {saida_png}")
    # print(f"Total de categorias mostradas: {len(categorias)}")
    # print(f"Categoria mais frequente: '{categorias[0]}' ({valores[0]:,} ocorrências)")




def criar_grafico_barra_horizontal(
    df: pd.DataFrame,
    coluna: str,
    titulo: str,
    nome_arquivo: str,
    top_n: int = 15,
    paleta: str = "Blues_r"
):
    """
    Gera um gráfico de barras horizontais com estilo acadêmico (TCC) e salva em PNG.
    Versão corrigida para trabalhar com dados pré-agregados.

    Parâmetros:
    -----------
    df : pd.DataFrame
        DataFrame contendo os dados.
    coluna : str
        Nome da coluna para plotar no eixo Y.
    titulo : str
        Título do gráfico.
    nome_arquivo : str
        Nome do arquivo de saída (sem caminho).
    top_n : int, opcional
        Número de top categorias para mostrar (padrão: 15).
    paleta : str, opcional
        Paleta de cores (padrão: 'Blues_r').
    """

    # Criar diretório de saída
    os.makedirs("resultados", exist_ok=True)

    # Configuração estética (padrão acadêmico)
    sns.set_theme(style="whitegrid", font_scale=1.2)
    
    # Calcular as top N categorias mais frequentes
    contagem = df[coluna].value_counts().head(top_n)
    
    # Se não houver dados suficientes, usar todos disponíveis
    if len(contagem) == 0:
        print(f"Nenhum dado encontrado na coluna: {coluna}")
        return
    
    # Preparar dados para o gráfico
    categorias = contagem.index.tolist()
    valores = contagem.values.tolist()
    
    # Criar figura com tamanho adequado para textos longos
    plt.figure(figsize=(14, 10))
    
    # CORREÇÃO: Usar barplot em vez de countplot
    ax = sns.barplot(
        y=categorias,
        x=valores,
        palette=paleta,
        orient='h'
    )

    # Adicionar rótulos nas barras
    for i, (p, valor) in enumerate(zip(ax.patches, valores)):
        ax.text(
            p.get_width() + p.get_width() * 0.01,
            p.get_y() + p.get_height() / 2,
            f"{int(valor):,}",
            ha='left', 
            va='center', 
            fontsize=10,
            weight='bold'
        )

    # Títulos e legendas claros
    plt.title(titulo, fontsize=16, weight="bold", pad=20)
    plt.xlabel("Quantidade", fontsize=12)
    plt.ylabel(coluna.replace("_", " ").title(), fontsize=12)
    
    # Ajustar limites do eixo X para caber os rótulos
    x_max = max(valores) * 1.15
    plt.xlim(0, x_max)
    
    # Grid mais suave
    plt.grid(axis='x', alpha=0.3)
    plt.grid(axis='y', alpha=0.1)
    
    # Layout mais ajustado
    plt.tight_layout()

    # Caminho completo do arquivo de saída
    saida_png = os.path.join("resultados", nome_arquivo)

    # Salvar imagem com qualidade TCC (300 DPI)
    plt.savefig(saida_png, dpi=300, bbox_inches="tight")
    plt.close()

    print(f" Gráfico horizontal em: {saida_png}")
    # print(f" Total de categorias mostradas: {len(categorias)}")
    # print(f" Categoria mais frequente: '{categorias[0]}' ({valores[0]:,} ocorrências)")