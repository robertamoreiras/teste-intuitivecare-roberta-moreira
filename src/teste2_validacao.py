"""
TESTE 2 - TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS
===========================================

Etapa 2.0: Setup e conferência inicial dos dados

Objetivo:
- Ler o CSV consolidado do Teste 1
- Ler os dados cadastrais das operadoras ANS
- Garantir que os arquivos estão corretos antes de aplicar validações

Nenhuma regra de negócio é aplicada aqui.
"""

import os
import pandas as pd


# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS (ROBUSTO)
# =============================================================================

# Diretório raiz do projeto (sobe um nível a partir de /src)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CAMINHO_CONSOLIDADO = os.path.join(
    BASE_DIR, "dados_consolidados", "consolidado_despesas.csv"
)

CAMINHO_CADASTRO = os.path.join(
    BASE_DIR, "dados_ans", "operadoras_ativas.csv"  # ajuste se o nome for diferente
)


# =============================================================================
# FUNÇÕES DE LEITURA
# =============================================================================

def carregar_csv(caminho, sep=";", encoding="utf-8"):
    """Carrega CSV com tratamento básico de erro"""
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    
    df = pd.read_csv(
        caminho,
        sep=sep,
        encoding=encoding,
        low_memory=False
    )
    return df


# =============================================================================
# ETAPA 2.0 - SETUP
# =============================================================================

def etapa_setup():
    print("=" * 70)
    print("TESTE 2 - ETAPA 2.0: SETUP E CONFERÊNCIA")
    print("=" * 70)
    print()

    # -------------------------------------------------------------------------
    # 1. Carregar CSV consolidado
    # -------------------------------------------------------------------------
    print("Carregando dados consolidados...")
    df_consolidado = carregar_csv(CAMINHO_CONSOLIDADO)

    print(f"✓ Linhas: {len(df_consolidado):,}")
    print(f"✓ Colunas: {list(df_consolidado.columns)}")
    print()

    # Conferência mínima esperada pelo teste
    colunas_esperadas = {
        "cnpj",
        "razaosocial",
        "trimestre",
        "ano",
        "valordespesas"
    }

    print("Conferindo colunas obrigatórias...")
    colunas_faltantes = colunas_esperadas - set(df_consolidado.columns)

    if colunas_faltantes:
        print("✗ Colunas ausentes:", colunas_faltantes)
    else:
        print("✓ Todas as colunas obrigatórias presentes")

    print()

    # -------------------------------------------------------------------------
    # 2. Carregar dados cadastrais das operadoras
    # -------------------------------------------------------------------------
    print("Carregando cadastro de operadoras ANS...")
    df_cadastro = carregar_csv(CAMINHO_CADASTRO)

    print(f"✓ Linhas: {len(df_cadastro):,}")
    print(f"✓ Colunas: {list(df_cadastro.columns)}")
    print()

    # Conferência mínima
    print("Amostra dos dados consolidados:")
    print(df_consolidado.head(3))
    print()

    print("Amostra dos dados cadastrais:")
    print(df_cadastro.head(3))
    print()

    print("Setup concluído com sucesso.")
    print("Próximas etapas:")
    print("- Validação de dados (2.1)")
    print("- Enriquecimento / Join (2.2)")
    print("- Agregação (2.3)")
    print()


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    etapa_setup()
