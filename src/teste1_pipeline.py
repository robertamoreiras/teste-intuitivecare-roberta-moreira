"""
PIPELINE SIMPLES - DEMONSTRAÇÕES CONTÁBEIS ANS
===============================================

Pipeline completo em código simples e direto para iniciantes.

PASSO A PASSO:
1. Baixa ZIPs usando wget (você cola os links)
2. Descompacta os ZIPs
3. Filtra arquivos de despesas/sinistros
4. Normaliza os arquivos

Autor: Roberta Moreira dos Santos
Data: 2026
"""

import os
import wget
import zipfile
import pandas as pd
import chardet
import re


# CONFIGURAÇÕES - AJUSTE AQUI

# COLE AQUI OS LINKS DOS 3 TRIMESTRES (visite o site da ANS e copie)
LINK1 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/1T2025.zip"
LINK2 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/2T2025.zip"
LINK3 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/3T2025.zip"

# Palavras-chave para filtrar despesas/sinistros
PALAVRAS_CHAVE = [
    "despesa", "despesas",
    "sinistro", "sinistros",
    "evento", "eventos",
    "atendimento", "atendimentos",
    "procedimento", "procedimentos",
    "utilizacao", "utilização"
]

# TRADE-OFF TÉCNICO
CHUNK_SIZE = 50000  # Linhas por chunk


# ETAPA 1: BAIXAR ARQUIVOS COM WGET
def etapa1_baixar_arquivos():
    """
    Baixa arquivos ZIP usando wget
    """
    print("=" * 70)
    print("ETAPA 1: BAIXANDO ARQUIVOS")
    print("=" * 70)
    print()
    
    # Criar pasta para downloads
    pasta = "dados_ans"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
        print(f"✓ Pasta '{pasta}' criada!")
    
    print()
    
    # Lista de links
    links = [LINK1, LINK2, LINK3]
    
    # Verificar se links foram preenchidos
    links_vazios = []
    for i, link in enumerate(links, 1):
        if not link.startswith("http"):
            links_vazios.append(i)
    
    if links_vazios:
        print("⚠ ATENÇÃO: Você precisa colar os links reais no código!")
        print(f"Links não preenchidos: {links_vazios}")
        print()
        print("Como fazer:")
        print("1. Acesse: https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/")
        print("2. Clique com botão direito nos arquivos ZIP")
        print("3. Copie o link")
        print("4. Cole nas variáveis LINK1, LINK2, LINK3 no topo do código")
        print()
        return False
    
    # Baixar cada arquivo
    sucessos = 0
    
    for i, link in enumerate(links, 1):
        if link.strip() == "":
            continue
        
        print(f"[{i}/3] Baixando...")
        print(f"URL: {link}")
        
        try:
            arquivo_baixado = wget.download(link, out=pasta)
            print()  # Pular linha após barra de progresso do wget
            
            tamanho_mb = os.path.getsize(arquivo_baixado) / (1024 * 1024)
            print(f"✓ Download concluído! ({tamanho_mb:.2f} MB)")
            sucessos += 1
            
        except Exception as e:
            print(f"✗ Erro ao baixar: {e}")
        
        print()
    
    print(f"Resumo: {sucessos}/3 arquivo(s) baixado(s)")
    print()
    
    return sucessos > 0


# ETAPA 2: DESCOMPACTAR ZIPS
def etapa2_descompactar():
    """
    Descompacta todos os arquivos ZIP
    """
    print("=" * 70)
    print("ETAPA 2: DESCOMPACTANDO ARQUIVOS")
    print("=" * 70)
    print()
    
    pasta_zips = "dados_ans"
    pasta_destino = "dados_extraidos"
    
    # Verificar se pasta de ZIPs existe
    if not os.path.exists(pasta_zips):
        print(f"✗ Pasta '{pasta_zips}' não encontrada!")
        print("Execute a Etapa 1 primeiro.")
        return False
    
    # Criar pasta de destino
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        print(f"✓ Pasta '{pasta_destino}' criada!")
    
    print()
    
    # Listar arquivos ZIP
    arquivos_zip = [f for f in os.listdir(pasta_zips) if f.endswith('.zip')]
    
    if not arquivos_zip:
        print("✗ Nenhum arquivo ZIP encontrado!")
        return False
    
    print(f"Encontrados {len(arquivos_zip)} arquivo(s) ZIP")
    print()
    
    # Descompactar cada um
    sucessos = 0
    
    for i, arquivo_zip in enumerate(arquivos_zip, 1):
        print(f"[{i}/{len(arquivos_zip)}] Descompactando: {arquivo_zip}")
        
        caminho_zip = os.path.join(pasta_zips, arquivo_zip)
        
        try:
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            
            print(f"  ✓ Descompactado com sucesso!")
            sucessos += 1
            
        except Exception as e:
            print(f"  ✗ Erro: {e}")
        
        print()
    
    print(f"Resumo: {sucessos}/{len(arquivos_zip)} arquivo(s) descompactado(s)")
    print()
    
    return sucessos > 0


# ETAPA 3: FILTRAR LINHAS DE DESPESAS / SINISTROS (CORRETA)
def etapa3_filtrar():
    print("=" * 70)
    print("ETAPA 3: FILTRANDO DESPESAS (CONTA CONTÁBIL) - INCREMENTAL")
    print("=" * 70)
    print()

    pasta_origem = "dados_extraidos"
    pasta_destino = "dados_despesas_sinistros"
    os.makedirs(pasta_destino, exist_ok=True)

    arquivos = []
    for raiz, _, files in os.walk(pasta_origem):
        for f in files:
            if f.endswith((".csv", ".txt", ".xlsx", ".xls")):
                arquivos.append(os.path.join(raiz, f))

    if not arquivos:
        print("✗ Nenhum arquivo encontrado.")
        return False

    total_despesas = 0

    for caminho in arquivos:
        nome = os.path.basename(caminho)
        ext = os.path.splitext(caminho)[1].lower()
        print(f"Processando: {nome}")

        nome_saida = nome
        if nome_saida.lower().endswith(".csv"):
            nome_saida = nome_saida.replace(".csv", "_despesas.csv")
        elif nome_saida.lower().endswith(".txt"):
            nome_saida = nome_saida.replace(".txt", "_despesas.csv")
        else:
            nome_saida = os.path.splitext(nome_saida)[0] + "_despesas.csv"

        caminho_saida = os.path.join(pasta_destino, nome_saida)

        # remove saída anterior se existir (pra não duplicar ao rerodar)
        if os.path.exists(caminho_saida):
            os.remove(caminho_saida)

        escreveu_header = False
        linhas_arquivo = 0

        try:
            if ext in [".csv", ".txt"]:
                # ✅ incremental de verdade
                for chunk in iterar_chunks_texto(caminho):
                    filtrado = filtrar_despesas_assistenciais(chunk)

                    if filtrado.empty:
                        continue

                    filtrado.to_csv(
                        caminho_saida,
                        sep=";",
                        encoding="utf-8",
                        index=False,
                        mode="a",
                        header=not escreveu_header
                    )
                    escreveu_header = True
                    linhas_arquivo += len(filtrado)

            else:
                # Excel (não incremental)
                df = pd.read_excel(caminho)
                df_filtrado = filtrar_despesas_assistenciais(df)

                if df_filtrado.empty:
                    print("  ✗ Nenhuma despesa encontrada")
                    continue

                df_filtrado.to_csv(caminho_saida, sep=";", encoding="utf-8", index=False)
                linhas_arquivo = len(df_filtrado)


        except Exception as e:
            print(f"  ✗ Erro ao processar: {e}")
            continue

        if linhas_arquivo == 0:
            print("  ✗ Nenhuma despesa encontrada")
            continue

        total_despesas += linhas_arquivo
        print(f"  ✓ {linhas_arquivo} linhas de despesa (salvo em {nome_saida})")

    print()
    print(f"Total de linhas de despesas: {total_despesas:,}")
    print()

    return total_despesas > 0


# ETAPA 4: NORMALIZAR ARQUIVOS
def detectar_encoding(caminho_arquivo):
    """Detecta encoding do arquivo"""
    try:
        with open(caminho_arquivo, 'rb') as f:
            resultado = chardet.detect(f.read(100000))
        return resultado['encoding']
    except:
        return 'utf-8'


def detectar_separador(caminho_arquivo, encoding):
    """Detecta separador CSV"""
    separadores = [';', ',', '|', '\t']
    
    try:
        with open(caminho_arquivo, 'r', encoding=encoding) as f:
            primeira_linha = f.readline()
        
        contagens = {}
        for sep in separadores:
            contagens[sep] = primeira_linha.count(sep)
        
        separador = max(contagens, key=contagens.get)
        
        if contagens[separador] == 0:
            return ';'
        
        return separador
    except:
        return ';'

def iterar_chunks_texto(caminho_arquivo, chunksize=CHUNK_SIZE):
    """
    Itera por chunks de CSV/TXT sem carregar o arquivo inteiro na memória.
    Detecta encoding e separador automaticamente.
    """
    encoding = detectar_encoding(caminho_arquivo)
    separador = detectar_separador(caminho_arquivo, encoding)

    for chunk in pd.read_csv(
        caminho_arquivo,
        sep=separador,
        encoding=encoding,
        on_bad_lines="skip",
        low_memory=False,
        chunksize=chunksize
    ):
        yield chunk


def ler_arquivo(caminho_arquivo):
    """
    Lê arquivo detectando formato automaticamente (modo simples).

    Observação:
    - Para processamento incremental (chunk por chunk), use iterar_chunks_texto()
      nas etapas do pipeline.
    """

    nome = os.path.basename(caminho_arquivo)
    extensao = os.path.splitext(caminho_arquivo)[1].lower()
    
    print(f"    Lendo: {nome}")
    
    try:
        # Excel
        if extensao in ['.xlsx', '.xls']:
            print(f"      Formato: Excel")
            df = pd.read_excel(caminho_arquivo)
            print(f"      ✓ Lido com sucesso!")
            return df
        
        # CSV ou TXT
        elif extensao in ['.csv', '.txt']:
            print(f"      Formato: Texto")
            
            # Detectar encoding
            encoding = detectar_encoding(caminho_arquivo)
            print(f"      Encoding: {encoding}")
            
            # Detectar separador
            separador = detectar_separador(caminho_arquivo, encoding)
            print(f"      Separador: '{separador}'")
            
            # lê completo (simples e previsível)
            df = pd.read_csv(
                caminho_arquivo,
                sep=separador,
                encoding=encoding,
                on_bad_lines="skip",
                low_memory=False
            )

            print(f"      ✓ Lido com sucesso!")
            return df
        
        else:
            print(f"      ✗ Formato não suportado: {extensao}")
            return None
            
    except Exception as e:
        print(f"      ✗ Erro: {e}")
        return None


def normalizar_colunas(df):
    df_norm = df.copy()

    df_norm.columns = (
        df_norm.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_', regex=False)
        .str.replace('ã', 'a', regex=False)
        .str.replace('õ', 'o', regex=False)
        .str.replace('á', 'a', regex=False)
        .str.replace('é', 'e', regex=False)
        .str.replace('í', 'i', regex=False)
        .str.replace('ó', 'o', regex=False)
        .str.replace('ú', 'u', regex=False)
        .str.replace('â', 'a', regex=False)
        .str.replace('ê', 'e', regex=False)
        .str.replace('ô', 'o', regex=False)
        .str.replace('ç', 'c', regex=False)
    )

    return df_norm


def filtrar_despesas_assistenciais(df):
    """
    Filtro mais aderente a 'assistenciais/sinistros' sem depender de um layout único.
    Estratégia:
    1) pega apenas contas da classe 3 (despesas)
    2) se existir coluna descritiva, filtra por PALAVRAS_CHAVE
    """
    df = normalizar_colunas(df)

    if "cd_conta_contabil" not in df.columns:
        return df.iloc[0:0]  # vazio

    df["cd_conta_contabil"] = df["cd_conta_contabil"].astype(str)
    df = df[df["cd_conta_contabil"].str.startswith("3")]

    if df.empty:
        return df

    # tenta achar uma coluna de texto para aplicar palavras-chave
    possiveis_colunas_texto = [
        "descricao", "ds_descricao", "descricao_conta", "ds_conta",
        "nome_conta", "conta", "ds_linha", "descricao_linha"
    ]
    col_texto = next((c for c in possiveis_colunas_texto if c in df.columns), None)

    # se não tiver coluna de texto, volta só com a regra da conta
    if not col_texto:
        return df

    # filtra por palavras-chave (case-insensitive)
    padrao = "|".join(re.escape(p) for p in PALAVRAS_CHAVE)
    mask = (
        df[col_texto]
        .astype(str)
        .str.lower()
        .str.contains(padrao, na=False)
    )
    return df[mask]


def etapa4_normalizar():
    print("=" * 70)
    print("ETAPA 4: NORMALIZANDO ARQUIVOS - INCREMENTAL")
    print("=" * 70)
    print()

    pasta_origem = "dados_despesas_sinistros"
    pasta_destino = "dados_normalizados"

    if not os.path.exists(pasta_origem):
        print(f"✗ Pasta '{pasta_origem}' não encontrada!")
        print("Execute a Etapa 3 primeiro.")
        return False

    os.makedirs(pasta_destino, exist_ok=True)

    arquivos = [f for f in os.listdir(pasta_origem) if f.endswith((".csv", ".txt", ".xlsx", ".xls"))]
    if not arquivos:
        print("✗ Nenhum arquivo encontrado!")
        return False

    print(f"Total de arquivos: {len(arquivos)}\n")

    processados = 0
    metadados = []

    for i, arquivo in enumerate(arquivos, 1):
        print(f"[{i}/{len(arquivos)}] Normalizando: {arquivo}")

        caminho_origem = os.path.join(pasta_origem, arquivo)
        ext = os.path.splitext(caminho_origem)[1].lower()

        nome_base = os.path.splitext(arquivo)[0]
        nome_saida = f"{nome_base}_normalizado.csv"
        caminho_saida = os.path.join(pasta_destino, nome_saida)

        # remove saída anterior se existir
        if os.path.exists(caminho_saida):
            os.remove(caminho_saida)

        escreveu_header = False
        linhas_total = 0
        colunas_total = None

        try:
            if ext in [".csv", ".txt"]:
                for chunk in iterar_chunks_texto(caminho_origem):
                    chunk_norm = normalizar_colunas(chunk)

                    if colunas_total is None:
                        colunas_total = len(chunk_norm.columns)

                    chunk_norm.to_csv(
                        caminho_saida,
                        sep=";",
                        encoding="utf-8",
                        index=False,
                        mode="a",
                        header=not escreveu_header
                    )
                    escreveu_header = True
                    linhas_total += len(chunk_norm)

            else:
                # Excel (não incremental)
                df = pd.read_excel(caminho_origem)
                df_norm = normalizar_colunas(df)
                linhas_total = len(df_norm)
                colunas_total = len(df_norm.columns)
                df_norm.to_csv(caminho_saida, sep=";", encoding="utf-8", index=False)

        except Exception as e:
            print(f"  ✗ Erro: {e}\n")
            continue

        tamanho_mb = os.path.getsize(caminho_saida) / (1024 * 1024)
        print(f"  ✓ Salvo: {nome_saida} | Linhas: {linhas_total:,} | Colunas: {colunas_total} | {tamanho_mb:.2f} MB\n")

        processados += 1
        metadados.append({
            "arquivo_original": arquivo,
            "arquivo_normalizado": nome_saida,
            "linhas": linhas_total,
            "colunas": colunas_total,
            "tamanho_mb": round(tamanho_mb, 2)
        })

    if metadados:
        df_meta = pd.DataFrame(metadados)
        caminho_relatorio = os.path.join(pasta_destino, "_RELATORIO.csv")
        df_meta.to_csv(caminho_relatorio, sep=";", encoding="utf-8", index=False)
        print("✓ Relatório salvo: _RELATORIO.csv\n")

    print(f"Resumo: {processados}/{len(arquivos)} arquivo(s) normalizado(s)\n")
    return processados > 0


# ETAPA 5: CONSOLIDAÇÃO FINAL
def etapa5_consolidar():
    print("\nETAPA 5: CONSOLIDAÇÃO FINAL - INCREMENTAL")
    print("=" * 60)

    pasta_entrada = "dados_despesas_sinistros"
    pasta_saida = "dados_consolidados"
    os.makedirs(pasta_saida, exist_ok=True)

    arquivo_csv = os.path.join(pasta_saida, "consolidado_despesas.csv")
    arquivo_zip = os.path.join(pasta_saida, "consolidado_despesas.zip")

    if not os.path.exists(pasta_entrada):
        print("❌ Pasta 'dados_despesas_sinistros' não encontrada.")
        return False

    arquivos = [ f for f in os.listdir(pasta_entrada)
        if f.endswith(".csv") and re.search(r"[1-4]T\d{4}", f)
    ]
    
    if not arquivos:
        print("❌ Nenhum arquivo de despesas encontrado para consolidar.")
        return False

    consolidados_por_arquivo = []

    for arquivo in arquivos:
        caminho = os.path.join(pasta_entrada, arquivo)
        print(f"→ Consolidando: {arquivo}")

        match = re.search(r"([1-4])T(\d{4})", arquivo)
        if not match:
            print("  ❌ Não foi possível identificar trimestre/ano pelo nome.")
            continue

        trimestre = f"{match.group(1)}T"
        ano = int(match.group(2))

        # acumulador: reg_ans -> soma(vl_saldo_final)
        acumulado = {}

        try:
            for chunk in iterar_chunks_texto(caminho):
                chunk = normalizar_colunas(chunk)

                colunas_necessarias = {"reg_ans", "vl_saldo_final"}
                if not colunas_necessarias.issubset(chunk.columns):
                    continue

                # numérico seguro
                vals = (
                    chunk["vl_saldo_final"]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )
                chunk["vl_saldo_final"] = pd.to_numeric(vals, errors="coerce")

                chunk = chunk[chunk["vl_saldo_final"] > 0]
                if chunk.empty:
                    continue

                # soma do chunk por reg_ans
                chunk["reg_ans"] = chunk["reg_ans"].astype(str)
                soma_chunk = chunk.groupby("reg_ans")["vl_saldo_final"].sum()

                # acumula em dict (leve)
                for reg, v in soma_chunk.items():
                    acumulado[reg] = acumulado.get(reg, 0.0) + float(v)

        except Exception as e:
            print(f"  ❌ Erro ao processar: {e}")
            continue

        if not acumulado:
            print("  ⚠️ Nenhum valor válido encontrado.")
            continue

        df_resumo = pd.DataFrame({
            "reg_ans": list(acumulado.keys()),
            "valor_despesas": list(acumulado.values()),
            "ano": ano,
            "trimestre": trimestre
        })

        consolidados_por_arquivo.append(df_resumo)

    if not consolidados_por_arquivo:
        print("❌ Nenhum dado válido consolidado.")
        return False

    df_final = pd.concat(consolidados_por_arquivo, ignore_index=True)
    df_final["valor_despesas"] = df_final["valor_despesas"].round(2)
    df_final = df_final.sort_values(by=["ano", "trimestre", "reg_ans"])

    df_final.to_csv(arquivo_csv, sep=";", index=False, encoding="utf-8")

    with zipfile.ZipFile(arquivo_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(arquivo_csv, arcname="consolidado_despesas.csv")

    print("\n✅ Consolidação incremental concluída!")
    print(f"📁 CSV gerado: {arquivo_csv}")
    print(f"🗜️ ZIP gerado: {arquivo_zip}")
    print(f"📊 Total de registros: {len(df_final)}")

    return True


# PIPELINE PRINCIPAL
def executar_pipeline():
    """
    Executa todas as etapas do pipeline
    """
    print()
    print("=" * 70)
    print("PIPELINE ANS - DEMONSTRAÇÕES CONTÁBEIS")
    print("=" * 70)
    print()
    
    # Etapa 1: Baixar
    if not etapa1_baixar_arquivos():
        print("Pipeline interrompido na Etapa 1")
        return
    
    # Etapa 2: Descompactar
    if not etapa2_descompactar():
        print("Pipeline interrompido na Etapa 2")
        return
    
    # Etapa 3: Filtrar
    if not etapa3_filtrar():
        print("Pipeline interrompido na Etapa 3")
        return
    
    # Etapa 4: Normalizar
    if not etapa4_normalizar():
        print("Pipeline interrompido na Etapa 4")
        return
    
    # Etapa 5: Consolidar
    if not etapa5_consolidar():
        print("Pipeline interrompido na Etapa 5")
        return
    
    # Sucesso!
    print("=" * 70)
    print("PIPELINE CONCLUÍDO COM SUCESSO!")
    print("=" * 70)
    print()
    print("Estrutura de pastas:")
    print("  • dados_ans/                  (ZIPs baixados)")
    print("  • dados_extraidos/            (Arquivos descompactados)")
    print("  • dados_despesas_sinistros/   (Filtrados)")
    print("  • dados_normalizados/         (Normalizados)")
    print("  • dados_consolidados/         (CSV + ZIP CONSOLIDADO)")
    print()


# PONTO DE ENTRADA
if __name__ == "__main__":
    # Verificar dependências
    try:
        import wget
        import pandas as pd
        import chardet
    except ImportError as e:
        print("=" * 70)
        print("ERRO: Bibliotecas necessárias não instaladas!")
        print("=" * 70)
        print()
        print("Execute:")
        print()
        print("  pip install wget pandas chardet openpyxl")
        print()
        print("=" * 70)
        exit(1)
    
    # Executar pipeline
    executar_pipeline()