import pandas as pd
import polars as pl
import os
import matplotlib.pyplot as plt
import seaborn as sns
from utils import criar_grafico_barra_horizontal

def analisar_mensagens_generic_events(df: pd.DataFrame):
    """
    Extrai mensagens entre ';' e '.' do full_log e gera gráfico das top 10
    """
    import re
    
    # Filtra apenas generic events
    generic_events = df[df["rule.description"].str.contains("Generic event", case=False, na=False)]
    
    if len(generic_events) == 0:
        print("Nenhum Generic Event encontrado")
        return
    
    # print(f"Encontrados {len(generic_events)} Generic Events")
    
    # Extrai mensagens entre ; e .
    mensagens = []
    for full_log in generic_events["full_log"]:
        if pd.isna(full_log):
            continue
            
        log_str = str(full_log)
        # Procura: texto; MENSAGEM.
        match = re.search(r';(.*?)\.', log_str)
        if match:
            mensagem = match.group(1).strip()
            if mensagem and len(mensagem) > 5:  # Filtra mensagens muito curtas
                mensagens.append(mensagem)
    
    if not mensagens:
        print("Nenhuma mensagem válida extraída")
        return
    
    # Cria DataFrame temporário para o gráfico
    df_mensagens = pd.DataFrame({'mensagem': mensagens})
    
    # Gera gráfico
    criar_grafico_barra_horizontal(
        df=df_mensagens,
        coluna="mensagem",
        titulo="Top 10 Mensagens de Generic Events",
        nome_arquivo="top10_mensagens_generic_events.png",
        top_n=10
    )

    # CORREÇÃO: Mostra as mensagens distintas extraídas
    # mensagens_distintas = df_mensagens['mensagem'].unique()
    # print("Mensagens distintas encontradas:")
    # for i, mensagem in enumerate(mensagens_distintas, 1):
    #     print(f"{i:2d}. {mensagem}")
    # Mostra contagem de todas as mensagens
    contagem = df_mensagens['mensagem'].value_counts()
    print("Contagem de todas as mensagens:")
    print(contagem.to_string())
    
    # Mostra apenas as quantidades
    # from collections import Counter
    # contador = Counter(mensagens)
    # print(f"\nTop 10 mensagens mais frequentes:")
    # for msg, count in contador.most_common(10):
    #     print(f"   {count:4d} x  {msg}")

