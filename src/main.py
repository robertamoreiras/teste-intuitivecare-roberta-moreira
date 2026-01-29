import pandas as pd
import os

pd.set_option('display.max_columns', None)

def ler_arquivo(caminho):
    ext = os.path.splitext(caminho)[1].lower()

    if ext == ".csv":
        return pd.read_csv(caminho, sep=";", encoding="latin-1")
    elif ext == ".txt":
        return pd.read_csv(caminho, sep="|", encoding="latin-1")
    elif ext == ".xlsx":
        return pd.read_excel(caminho)
    else:
        raise ValueError(f"Formato não suportado: {ext}")

arquivos = ["data/extracted/1T2025/1T2025.csv", "data/extracted/2T2025/2T2025.csv", "data/extracted/3T2025/3T2025.csv"]

for caminho in arquivos:
    df = ler_arquivo(caminho)
    print(f"Arquivo lido: {caminho}")
    print(df.head())

# def filtrar_arquivo(caminho):
#     filtro = tabela_df['DESCRICAO'].str.contains('EVENTOS/SINISTROS', case=False, na=False)
#     despesas_df = tabela_df[filtro]
#     print(despesas_df)