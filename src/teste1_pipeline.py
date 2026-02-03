"""
PIPELINE SIMPLES - DEMONSTRAÇÕES CONTÁBEIS ANS
===============================================

Pipeline completo em código simples e direto para iniciantes.
Executa todas as etapas da Parte 1 do desafio.

PASSO A PASSO:
1. Baixa ZIPs usando wget (você cola os links)
2. Descompacta os ZIPs
3. Filtra arquivos de despesas/sinistros
4. Normaliza os arquivos

Autor: Pipeline ANS Simplificado
Data: 2026
"""

import os
import wget
import zipfile
import shutil
import pandas as pd
import chardet
import re


# =============================================================================
# CONFIGURAÇÕES - AJUSTE AQUI
# =============================================================================

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
# True = Processa em chunks (economiza memória, mais lento)
# False = Carrega tudo de uma vez (usa mais memória, mais rápido)
PROCESSAR_INCREMENTAL = True
CHUNK_SIZE = 50000  # Linhas por chunk


# =============================================================================
# ETAPA 1: BAIXAR ARQUIVOS COM WGET
# =============================================================================

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
        if "ARQUIVO" in link:
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


# =============================================================================
# ETAPA 2: DESCOMPACTAR ZIPS
# =============================================================================

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

# =============================================================================
# ETAPA 3: FILTRAR LINHAS DE DESPESAS / SINISTROS (CORRETA)
# =============================================================================

def etapa3_filtrar():
    print("=" * 70)
    print("ETAPA 3: FILTRANDO DESPESAS (CONTA CONTÁBIL)")
    print("=" * 70)
    print()

    pasta_origem = "dados_extraidos"
    pasta_destino = "dados_despesas_sinistros"
    os.makedirs(pasta_destino, exist_ok=True)

    arquivos = []
    for raiz, _, files in os.walk(pasta_origem):
        for f in files:
            if f.endswith((".csv", ".txt")):
                arquivos.append(os.path.join(raiz, f))

    if not arquivos:
        print("✗ Nenhum arquivo encontrado.")
        return False

    total_despesas = 0

    for caminho in arquivos:
        nome = os.path.basename(caminho)
        print(f"Processando: {nome}")

        df = ler_arquivo(caminho)
        if df is None or df.empty:
            print("  ✗ Arquivo inválido")
            continue

        df = normalizar_colunas(df)

        # 🔑 REGRA CONTÁBIL REAL
        # Despesas = contas que começam com "3"
        if "cd_conta_contabil" not in df.columns:
            print("  ✗ Coluna cd_conta_contabil não encontrada")
            continue

        df["cd_conta_contabil"] = df["cd_conta_contabil"].astype(str)

        df_despesas = df[df["cd_conta_contabil"].str.startswith("3")]

        if df_despesas.empty:
            print("  ✗ Nenhuma despesa encontrada")
            continue

        nome_saida = nome.replace(".csv", "_despesas.csv")
        caminho_saida = os.path.join(pasta_destino, nome_saida)

        df_despesas.to_csv(
            caminho_saida,
            sep=";",
            encoding="utf-8",
            index=False
        )

        total_despesas += len(df_despesas)
        print(f"  ✓ {len(df_despesas)} linhas de despesa")

    print()
    print(f"Total de linhas de despesas: {total_despesas:,}")
    print()

    return total_despesas > 0

# =============================================================================
# ETAPA 4: NORMALIZAR ARQUIVOS
# =============================================================================

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


def ler_arquivo(caminho_arquivo):
    """
    Lê arquivo detectando formato automaticamente
    
    TRADE-OFF TÉCNICO:
    - Se PROCESSAR_INCREMENTAL=True: Lê em chunks (economiza memória)
    - Se PROCESSAR_INCREMENTAL=False: Lê tudo de uma vez (mais rápido)
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
            
            # AQUI ESTÁ O TRADE-OFF!
            if PROCESSAR_INCREMENTAL:
                print(f"      Modo: Incremental (chunks de {CHUNK_SIZE} linhas)")
                print(f"      Motivo: Economiza memória, processa arquivos grandes")
                
                chunks = []
                chunk_count = 0
                
                for chunk in pd.read_csv(
                    caminho_arquivo,
                    sep=separador,
                    encoding=encoding,
                    on_bad_lines='skip',
                    low_memory=False,
                    chunksize=CHUNK_SIZE
                ):
                    chunks.append(chunk)
                    chunk_count += 1
                    print(f"      Processando chunk {chunk_count}...", end='\r')
                
                print(f"      ✓ {chunk_count} chunks processados          ")
                df = pd.concat(chunks, ignore_index=True)
                
            else:
                print(f"      Modo: Completo (carrega tudo)")
                print(f"      Motivo: Mais rápido para arquivos pequenos")
                
                df = pd.read_csv(
                    caminho_arquivo,
                    sep=separador,
                    encoding=encoding,
                    on_bad_lines='skip',
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
    """Normaliza nomes das colunas"""
    df_norm = df.copy()
    
    df_norm.columns = (
        df_norm.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('ã', 'a')
        .str.replace('õ', 'o')
        .str.replace('á', 'a')
        .str.replace('é', 'e')
        .str.replace('í', 'i')
        .str.replace('ó', 'o')
        .str.replace('ú', 'u')
        .str.replace('â', 'a')
        .str.replace('ê', 'e')
        .str.replace('ô', 'o')
        .str.replace('ç', 'c')
    )
    
    return df_norm


def etapa4_normalizar():
    """
    Normaliza todos os arquivos para formato padrão
    """
    print("=" * 70)
    print("ETAPA 4: NORMALIZANDO ARQUIVOS")
    print("=" * 70)
    print()
    
    # Mostrar configuração do trade-off
    print("TRADE-OFF TÉCNICO - Processamento:")
    print("-" * 70)
    if PROCESSAR_INCREMENTAL:
        print("✓ Modo: INCREMENTAL (chunk por chunk)")
        print("  Vantagens: Usa menos memória, processa arquivos grandes")
        print("  Desvantagens: ~20% mais lento")
    else:
        print("✓ Modo: COMPLETO (tudo em memória)")
        print("  Vantagens: Mais rápido")
        print("  Desvantagens: Usa muita memória, pode falhar em arquivos grandes")
    print("-" * 70)
    print()
    
    pasta_origem = "dados_despesas_sinistros"
    pasta_destino = "dados_normalizados"
    
    # Verificar pasta de origem
    if not os.path.exists(pasta_origem):
        print(f"✗ Pasta '{pasta_origem}' não encontrada!")
        print("Execute a Etapa 3 primeiro.")
        return False
    
    # Criar pasta de destino
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        print(f"✓ Pasta '{pasta_destino}' criada!")
    
    print()
    
    # Listar arquivos para normalizar
    arquivos = [
        f for f in os.listdir(pasta_origem)
        if f.endswith(('.csv', '.txt', '.xlsx', '.xls'))
    ]
    
    if not arquivos:
        print("✗ Nenhum arquivo encontrado!")
        return False
    
    print(f"Total de arquivos: {len(arquivos)}")
    print()
    
    # Processar cada arquivo
    processados = 0
    metadados = []
    
    for i, arquivo in enumerate(arquivos, 1):
        print(f"[{i}/{len(arquivos)}] Normalizando: {arquivo}")
        
        caminho_origem = os.path.join(pasta_origem, arquivo)
        
        # Ler arquivo
        df = ler_arquivo(caminho_origem)
        
        if df is None:
            print()
            continue
        
        print(f"      Linhas: {len(df):,} | Colunas: {len(df.columns)}")
        
        # Normalizar colunas
        print(f"      Normalizando colunas...")
        df_norm = normalizar_colunas(df)
        
        # Salvar
        nome_base = os.path.splitext(arquivo)[0]
        nome_saida = f"{nome_base}_normalizado.csv"
        caminho_saida = os.path.join(pasta_destino, nome_saida)
        
        df_norm.to_csv(caminho_saida, sep=';', encoding='utf-8', index=False)
        
        tamanho_mb = os.path.getsize(caminho_saida) / (1024 * 1024)
        print(f"      ✓ Salvo ({tamanho_mb:.2f} MB)")
        
        processados += 1
        
        # Guardar metadados
        metadados.append({
            'arquivo_original': arquivo,
            'arquivo_normalizado': nome_saida,
            'linhas': len(df),
            'colunas': len(df.columns),
            'tamanho_mb': round(tamanho_mb, 2)
        })
        
        print()
    
    # Salvar relatório
    if metadados:
        df_meta = pd.DataFrame(metadados)
        caminho_relatorio = os.path.join(pasta_destino, '_RELATORIO.csv')
        df_meta.to_csv(caminho_relatorio, sep=';', encoding='utf-8', index=False)
        print(f"✓ Relatório salvo: _RELATORIO.csv")
        print()
    
    print(f"Resumo: {processados}/{len(arquivos)} arquivo(s) normalizado(s)")
    print()
    
    return processados > 0

# =============================================================================
# ETAPA 5: CONSOLIDAÇÃO FINAL (DESPESAS / EVENTOS ANS)
# =============================================================================

def etapa5_consolidar():
    print("\nETAPA 5: CONSOLIDAÇÃO FINAL - DESPESAS ASSISTENCIAIS")
    print("=" * 60)

    pasta_entrada = "dados_despesas_sinistros"
    pasta_saida = "dados_consolidados"
    os.makedirs(pasta_saida, exist_ok=True)

    arquivo_csv = os.path.join(pasta_saida, "consolidado_despesas.csv")
    arquivo_zip = os.path.join(pasta_saida, "consolidado_despesas.zip")

    if not os.path.exists(pasta_entrada):
        print("❌ Pasta 'dados_despesas_sinistros' não encontrada.")
        return False

    arquivos = [f for f in os.listdir(pasta_entrada) if f.endswith(".csv")]

    if not arquivos:
        print("❌ Nenhum arquivo de despesas encontrado para consolidar.")
        return False

    consolidados = []

    for arquivo in arquivos:
        caminho = os.path.join(pasta_entrada, arquivo)
        print(f"→ Consolidando: {arquivo}")

        try:
            df = pd.read_csv(caminho, sep=";", encoding="latin-1")
        except Exception as e:
            print(f"  ❌ Erro ao ler arquivo: {e}")
            continue

        # Validação mínima da estrutura
        colunas_necessarias = {"data", "reg_ans", "vl_saldo_final"}
        if not colunas_necessarias.issubset(df.columns):
            print("  ❌ Estrutura incompatível, ignorado")
            continue

        # Garantir tipo numérico (se vier com vírgula, tenta corrigir)
        df["vl_saldo_final"] = (
            df["vl_saldo_final"]
            .astype(str)
            .str.replace(".", "", regex=False)   # remove separador de milhar (se existir)
            .str.replace(",", ".", regex=False)  # troca vírgula por ponto
        )
        df["vl_saldo_final"] = pd.to_numeric(df["vl_saldo_final"], errors="coerce")

        # Remover valores nulos ou <= 0
        df = df[df["vl_saldo_final"] > 0]

        if df.empty:
            print("  ⚠️ Arquivo sem valores válidos após filtro.")
            continue

        # Extrair ano e trimestre do nome do arquivo (ex: 1T2025)
        match = re.search(r"([1-4])T(\d{4})", arquivo)
        if not match:
            print("  ❌ Não foi possível identificar trimestre/ano pelo nome.")
            continue

        trimestre = f"{match.group(1)}T"
        ano = int(match.group(2))

        # Consolidação (somatório por operadora)
        resumo = (
            df.groupby("reg_ans", as_index=False)["vl_saldo_final"]
            .sum()
            .rename(columns={"vl_saldo_final": "valor_despesas"})
        )

        resumo["ano"] = ano
        resumo["trimestre"] = trimestre

        consolidados.append(resumo)

    if not consolidados:
        print("❌ Nenhum dado válido consolidado.")
        return False

    df_final = pd.concat(consolidados, ignore_index=True)

    # Ordenar para facilitar leitura
    df_final = df_final.sort_values(by=["ano", "trimestre", "reg_ans"])

    # Salvar CSV
    df_final.to_csv(arquivo_csv, sep=";", index=False, encoding="utf-8")

    # Criar ZIP com o CSV dentro
    with zipfile.ZipFile(arquivo_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(arquivo_csv, arcname="consolidado_despesas.csv")

    print("\n✅ Consolidação concluída com sucesso!")
    print(f"📁 CSV gerado: {arquivo_csv}")
    print(f"🗜️ ZIP gerado: {arquivo_zip}")
    print(f"📊 Total de registros: {len(df_final)}")

    return True

# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================

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
    print("  • dados_normalizados/         (Normalizados - PRONTOS!)")
    print("  • dados_normalizados/         (Normalizados)")
    print("  • saida_final/                (CSV + ZIP CONSOLIDADO)")
    print()

# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

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