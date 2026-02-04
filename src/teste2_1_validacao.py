import os
import pandas as pd


# CONFIGURAÇÃO
ARQUIVO_ENTRADA = "dados_consolidados/consolidado_despesas.csv"
PASTA_SAIDA = "dados_validados"

os.makedirs(PASTA_SAIDA, exist_ok=True)


# FUNÇÕES DE VALIDAÇÃO
def validar_ano(valor):
    if pd.isna(valor):
        return False
    try:
        ano = int(valor)
    except (ValueError, TypeError):
        return False
    return 2000 <= ano <= 2100

def validar_trimestre(valor):
    if pd.isna(valor):
        return False
    t = str(valor).strip().upper()
    return t in {"1T", "2T", "3T", "4T"}

def validar_valor(valor):
    if pd.isna(valor):
        return False
    try:
        v = float(valor)
    except (ValueError, TypeError):
        return False
    return v > 0


# PIPELINE DE VALIDAÇÃO
def validar_dados():
    print("\nTESTE 2.1 — VALIDAÇÃO DE DADOS")
    print("=" * 50)

    if not os.path.exists(ARQUIVO_ENTRADA):
        print("❌ Arquivo consolidado não encontrado.")
        return False

    df = pd.read_csv(
        ARQUIVO_ENTRADA,
        sep=";",
        dtype={"reg_ans": str, "trimestre": str},
    )

    # Normalização básica
    df.columns = df.columns.str.lower()

    # Flags de validação
    df["ano_valido"] = df["ano"].apply(validar_ano)
    df["trimestre_valido"] = df["trimestre"].apply(validar_trimestre)
    df["valor_valido"] = df["valor_despesas"].apply(validar_valor)
    df["reg_ans_valido"] = df["reg_ans"].astype(str).str.strip().ne("") & df["reg_ans"].notna()

    # Registro válido se TODAS as regras passarem
    df["registro_valido"] = (
        df["ano_valido"] &
        df["trimestre_valido"] &
        df["valor_valido"] &
        df["reg_ans_valido"]
    )

    # Separação
    df_validos = df[df["registro_valido"]].copy()
    df_invalidos = df[~df["registro_valido"]].copy()

    # Remover colunas técnicas
    colunas_remover = [
        "ano_valido",
        "trimestre_valido",
        "valor_valido",
        "reg_ans_valido",
        "registro_valido"
    ]

    df_validos.drop(columns=colunas_remover, inplace=True)
    df_invalidos.drop(columns=colunas_remover, inplace=True)

    # Salvar resultados
    caminho_validos = os.path.join(PASTA_SAIDA, "despesas_validadas.csv")
    caminho_invalidos = os.path.join(PASTA_SAIDA, "despesas_invalidas.csv")

    df_validos.to_csv(caminho_validos, sep=";", index=False, encoding="utf-8")
    df_invalidos.to_csv(caminho_invalidos, sep=";", index=False, encoding="utf-8")

    print(f"✅ Registros válidos: {len(df_validos)}")
    print(f"⚠️ Registros inválidos: {len(df_invalidos)}")
    print(f"📁 Saída: {PASTA_SAIDA}/")

    return True


if __name__ == "__main__":
    validar_dados()
