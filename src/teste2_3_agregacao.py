import os
import pandas as pd
import zipfile


# CONFIGURAÇÃO
ARQUIVO_ENTRADA = "dados_enriquecidos/consolidado_enriquecido.csv"
PASTA_SAIDA = "dados_agregados"
os.makedirs(PASTA_SAIDA, exist_ok=True)

ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, "despesas_agregadas.csv")
ARQUIVO_ZIP = os.path.join(PASTA_SAIDA, "Teste_Roberta_Moreira.zip")


# PIPELINE DE AGREGAÇÃO
def executar_agregacao():
    print("\nTESTE 2.3 — AGREGAÇÃO DE DESPESAS")
    print("=" * 60)

    if not os.path.exists(ARQUIVO_ENTRADA):
        print("❌ Arquivo enriquecido não encontrado.")
        return False


    # 1) Leitura
    df = pd.read_csv(ARQUIVO_ENTRADA, sep=";", encoding="utf-8")

    # Normalização defensiva
    df.columns = df.columns.str.lower()

    colunas_necessarias = {
        "razao_social",
        "uf",
        "ano",
        "trimestre",
        "valor_despesas"
    }

    if not colunas_necessarias.issubset(df.columns):
        print("❌ Estrutura inesperada.")
        print("Colunas encontradas:", list(df.columns))
        return False

    # Garantir tipos corretos
    df["valor_despesas"] = (
        df["valor_despesas"].astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["valor_despesas"] = pd.to_numeric(df["valor_despesas"], errors="coerce")
    df = df.dropna(subset=["valor_despesas"])

    # manter só registros com match OK no cadastro
    if "status_match" in df.columns:
        df["status_match"] = df["status_match"].astype(str).str.strip().str.upper()
        df = df[df["status_match"] == "OK"].copy()

    # normalizar campos de texto
    df["razao_social"] = df["razao_social"].fillna("").astype(str).str.strip()
    df["uf"] = df["uf"].fillna("").astype(str).str.strip().str.upper()

    # manter apenas registros com dados mínimos
    df = df[(df["razao_social"] != "") & (df["uf"] != "")].copy()


    # 2) AGREGAÇÃO PRINCIPAL
    # Agrupamento por Operadora (Razão Social) e UF
    agrupado = (
        df
        .groupby(["razao_social", "uf"], dropna=False)
        .agg(
            total_despesas=("valor_despesas", "sum"),
            media_trimestral=("valor_despesas", "mean"),
            desvio_padrao=("valor_despesas", "std"),
            quantidade_registros=("valor_despesas", "count")
        )
        .reset_index()
    )

    # Arredondamento financeiro
    agrupado["total_despesas"] = agrupado["total_despesas"].round(2)
    agrupado["media_trimestral"] = agrupado["media_trimestral"].round(2)
    agrupado["desvio_padrao"] = agrupado["desvio_padrao"].round(2)


    # 3) ORDENAÇÃO
    # Ordenar do maior para o menor gasto total
    agrupado = agrupado.sort_values(
        by="total_despesas",
        ascending=False
    )


    # 4) SALVAR RESULTADO
    agrupado.to_csv(
        ARQUIVO_SAIDA,
        sep=";",
        index=False,
        encoding="utf-8"
    )

    # Compactar
    with zipfile.ZipFile(ARQUIVO_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(ARQUIVO_SAIDA, arcname="despesas_agregadas.csv")

    print("✅ Agregação concluída com sucesso!")
    print(f"📄 CSV: {ARQUIVO_SAIDA}")
    print(f"📦 ZIP: {ARQUIVO_ZIP}")
    print(f"📊 Operadoras/UF distintas: {len(agrupado)}")

    return True


if __name__ == "__main__":
    executar_agregacao()
