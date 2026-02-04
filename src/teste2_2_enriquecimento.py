import os
import pandas as pd
import wget


# CONFIGURAÇÃO
# Entrada validada no Teste 2.1
ARQUIVO_ENTRADA = "dados_validados/despesas_validadas.csv"
PASTA_SAIDA = "dados_enriquecidos"
os.makedirs(PASTA_SAIDA, exist_ok=True)

# Cadastro ANS (operadoras ativas)
CAD_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
CAD_LOCAL = os.path.join(PASTA_SAIDA, "Relatorio_cadop.csv")

ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, "consolidado_enriquecido.csv")


# HELPERS
def normalizar_nome_coluna(s: str) -> str:
    s = str(s).strip().lower()
    trocas = {
        "ã": "a", "á": "a", "â": "a", "à": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for a, b in trocas.items():
        s = s.replace(a, b)
    s = s.replace(" ", "_")
    return s


def somente_digitos(x) -> str:
    if pd.isna(x):
        return ""
    return "".join(ch for ch in str(x) if ch.isdigit())


def escolher_modo_ou_primeiro(serie: pd.Series):
    # Retorna o valor mais frequente não-nulo; se empatar, retorna o primeiro ordenado.
    vals = [v for v in serie.dropna().astype(str).tolist() if v.strip() != ""]
    if not vals:
        return None
    vc = pd.Series(vals).value_counts()
    top = vc[vc == vc.max()].index.tolist()
    return sorted(top)[0]


def baixar_cadastro_se_precisar():
    if os.path.exists(CAD_LOCAL) and os.path.getsize(CAD_LOCAL) > 0:
        print("✓ Cadastro já existe localmente.")
        return
    print(f"Baixando cadastro ANS...\nURL: {CAD_URL}")
    wget.download(CAD_URL, out=CAD_LOCAL)
    print("\n✓ Download OK")


def achar_coluna(df: pd.DataFrame, termos):
    # Encontra a primeira coluna que contém TODOS os termos em 'termos'
    # Ex: termos=["registro"] encontra "registro_ans" ou "registroans" etc.
   
    for col in df.columns:
        if all(t in col for t in termos):
            return col
    return None


def ler_cadastro_robusto(caminho: str) -> pd.DataFrame:
    # Lê o cadastro tentando encodings e separadores comuns.
    tentativas = [
        {"sep": ";", "encoding": "utf-8-sig"},
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ";", "encoding": "latin-1"},
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ",", "encoding": "latin-1"},
    ]

    ultimo_erro = None
    for t in tentativas:
        try:
            df = pd.read_csv(caminho, dtype=str, **t)
            # se veio com 1 coluna só, provavelmente separador errado
            if df.shape[1] > 1:
                print(f"✓ Cadastro lido com sep='{t['sep']}' encoding='{t['encoding']}'")
                return df
        except Exception as e:
            ultimo_erro = e

    raise RuntimeError(f"Não consegui ler o cadastro. Último erro: {ultimo_erro}")


# ENRIQUECIMENTO
def executar_enriquecimento():
    print("\nTESTE 2.2 — ENRIQUECIMENTO COM CADASTRO ANS")
    print("=" * 65)

    # 1) Ler entrada (consolidado validado)
    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"❌ Arquivo de entrada não encontrado: {ARQUIVO_ENTRADA}")
        print("Dica: rode o Teste 2.1 antes, ou aponte para o consolidado.")
        return False

    df_cons = pd.read_csv(ARQUIVO_ENTRADA, sep=";", encoding="utf-8")
    df_cons.columns = [normalizar_nome_coluna(c) for c in df_cons.columns]

    required = {"reg_ans", "ano", "trimestre", "valor_despesas"}
    if not required.issubset(df_cons.columns):
        print("❌ Entrada com colunas inesperadas.")
        print("   Colunas encontradas:", list(df_cons.columns))
        return False

    df_cons["reg_ans"] = df_cons["reg_ans"].astype(str).str.strip()
    df_cons["valor_despesas"] = pd.to_numeric(df_cons["valor_despesas"], errors="coerce").round(2)

    # 2) Baixar + ler cadastro ANS
    baixar_cadastro_se_precisar()

    df_cad = ler_cadastro_robusto(CAD_LOCAL)
    df_cad.columns = [normalizar_nome_coluna(c) for c in df_cad.columns]

    # 3) Detectar colunas do cadastro
    col_reg = achar_coluna(df_cad, ["registro"]) or achar_coluna(df_cad, ["reg"])
    col_cnpj = achar_coluna(df_cad, ["cnpj"])
    col_razao = achar_coluna(df_cad, ["razao"]) or achar_coluna(df_cad, ["nome"])
    col_mod = achar_coluna(df_cad, ["modalidade"])
    col_uf = achar_coluna(df_cad, ["uf"])

    if any(c is None for c in [col_reg, col_cnpj, col_mod, col_uf]):
        print("❌ Não consegui identificar colunas essenciais no cadastro.")
        print("   Colunas encontradas:", list(df_cad.columns))
        print("   Detectadas:", {"registro": col_reg, "cnpj": col_cnpj, "modalidade": col_mod, "uf": col_uf, "razao": col_razao})
        return False

    if col_razao is None:
        df_cad["razao_social_dummy"] = None
        col_razao = "razao_social_dummy"

    # 4) Normalizar chaves do cadastro
    df_cad[col_reg] = df_cad[col_reg].astype(str).str.strip()
    df_cad[col_cnpj] = df_cad[col_cnpj].astype(str).str.strip()

    df_cad["cnpj_norm"] = df_cad[col_cnpj].apply(somente_digitos)
    df_cad_base = df_cad[df_cad["cnpj_norm"] != ""].copy()

    # 5) Resolver duplicidade no cadastro por CNPJ (CORRIGIDO)
    # Usa grp.name como chave do grupo (sempre existe)
    campos = {
        "registro_ans": col_reg,
        "modalidade": col_mod,
        "uf": col_uf,
        "razao_social": col_razao,
    }

    def agregador(grp: pd.DataFrame) -> pd.Series:
        out = {}
        status_dup = False

        # ✅ chave do grupo (cnpj_norm)
        cnpj_val = str(grp.name)

        for out_name, col in campos.items():
            uniques = grp[col].dropna().astype(str).str.strip()
            uniques = uniques[uniques != ""].unique().tolist()
            if len(uniques) > 1:
                status_dup = True

            out[out_name] = escolher_modo_ou_primeiro(grp[col])

        out["cnpj"] = cnpj_val
        out["status_cadastro"] = "CADASTRO_DUPLICADO" if status_dup else "OK"
        return pd.Series(out)

    df_cad_agg = (
        df_cad_base
        .groupby("cnpj_norm")          # ✅ importante para grp.name
        .apply(agregador)
        .reset_index(drop=True)
    )

    # 6) Trazer CNPJ para o consolidado via RegistroANS (reg_ans)
    mapa_reg_cnpj = (
        df_cad_base[[col_reg, "cnpj_norm"]]
        .dropna()
        .drop_duplicates(subset=[col_reg], keep="first")
        .rename(columns={col_reg: "reg_ans", "cnpj_norm": "cnpj"})
    )

    df_tmp = df_cons.merge(mapa_reg_cnpj, on="reg_ans", how="left")
    df_tmp["cnpj"] = df_tmp["cnpj"].fillna("").astype(str)

    # 7) Join FINAL por CNPJ (pedido do enunciado)
    df_enriq = df_tmp.merge(
        df_cad_agg[["cnpj", "registro_ans", "modalidade", "uf", "razao_social", "status_cadastro"]],
        on="cnpj",
        how="left"
    )

    # 8) Tratar sem match
    df_enriq["status_match"] = "OK"
    df_enriq.loc[df_enriq["cnpj"].eq(""), "status_match"] = "SEM_CNPJ_NO_CONSOLIDADO"
    df_enriq.loc[df_enriq["cnpj"].ne("") & df_enriq["registro_ans"].isna(), "status_match"] = "SEM_MATCH_NO_CADASTRO"

    # 9) Salvar saída enriquecida
    colunas_saida = [
        "cnpj",
        "razao_social",
        "registro_ans",
        "modalidade",
        "uf",
        "ano",
        "trimestre",
        "valor_despesas",
        "status_match",
        "status_cadastro",
    ]

    df_enriq[colunas_saida].to_csv(ARQUIVO_SAIDA, sep=";", index=False, encoding="utf-8")

    print("\n✅ Enriquecimento concluído!")
    print(f"📄 Saída: {ARQUIVO_SAIDA}")

    print("\nResumo status_match:")
    print(df_enriq["status_match"].value_counts(dropna=False).to_string())

    print("\nResumo status_cadastro:")
    print(df_enriq["status_cadastro"].fillna("SEM_INFO").value_counts().to_string())

    return True


if __name__ == "__main__":
    executar_enriquecimento()
