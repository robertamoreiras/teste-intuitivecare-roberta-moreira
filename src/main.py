import pandas as pd
import os
import wget
import zipfile
import shutil
import chardet

pd.set_option('display.max_columns', None)

link1 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/1T2025.zip"

link2 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/2T2025.zip"

link3 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/2025/3T2025.zip"

def baixar_arquivo(url, pasta_destino="dados_ans"):
    # Criar pasta se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        print(f"✓ Pasta '{pasta_destino}' criada!\n")
    
    # Extrair nome do arquivo da URL
    nome_arquivo = url.split('/')[-1]
    
    print(f"Baixando: {nome_arquivo}")
    print(f"URL: {url}")
    print()
    
    try:
        # Baixar arquivo com wget (mostra barra de progresso automaticamente)
        arquivo_baixado = wget.download(url, out=pasta_destino)
        
        # Pular linha depois da barra de progresso
        print()
        
        return True
        
    except Exception as e:
        print(f"✗ Erro ao baixar: {e}")
        print()
        return False

def main():
    # Lista com os 3 links
    links = [link1, link2, link3]
    
    # Verificar se os links foram substituídos
    links_vazios = []
    for i, link in enumerate(links, 1):
        if "ARQUIVO" in link or link.strip() == "":
            links_vazios.append(i)
    
    if links_vazios:
        print("⚠ ATENÇÃO: Você precisa substituir os links antes de executar!")
        print(f"Links não preenchidos: {links_vazios}")
        print()
        print("COMO PREENCHER OS LINKS:")
        print("1. Acesse: https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/")
        print("2. Clique com botão direito nos arquivos ZIP")
        print("3. Selecione 'Copiar endereço do link' ou 'Copiar link'")
        print("4. Cole o link nas variáveis link1, link2, link3 neste arquivo")
        print()
        print("EXEMPLO:")
        print('link1 = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/demo_2024_4T.zip"')
        print()
        return
    
    # Contar quantos links válidos temos
    links_validos = [link for link in links if link.strip() != ""]
    total = len(links_validos)
    
    print(f"Total de arquivos para baixar: {total}")
    print()
    
    # Baixar cada arquivo
    sucessos = 0
    
    for i, link in enumerate(links_validos, 1):
        print(f"[Arquivo {i}/{total}] " + "-" * 55)
        
        if baixar_arquivo(link):
            sucessos += 1
    
    # Mostrar resumo final
    print("=" * 70)
    print("RESUMO DO DOWNLOAD")
    print("=" * 70)
    print(f"Arquivos baixados com sucesso: {sucessos}/{total}")
    print(f"Localização dos arquivos: {os.path.abspath('dados_ans')}")
    print()
    
    # Listar os arquivos baixados
    if os.path.exists('dados_ans'):
        arquivos_zip = [f for f in os.listdir('dados_ans') if f.endswith('.zip')]
        
        if arquivos_zip:
            print("Arquivos na pasta:")
            for arquivo in sorted(arquivos_zip, reverse=True):
                caminho_completo = os.path.join('dados_ans', arquivo)
                tamanho_mb = os.path.getsize(caminho_completo) / (1024 * 1024)
                print(f"  • {arquivo} ({tamanho_mb:.2f} MB)")
            print()

def descompactar_zips():
    """
    Descompacta todos os arquivos ZIP da pasta dados_ans
    """
    
    print("=" * 70)
    print("DESCOMPACTADOR DE ARQUIVOS ANS")
    print("=" * 70)
    print()
    
    # Pasta onde estão os ZIPs
    pasta_zips = "dados_ans"
    
    # Pasta onde serão extraídos os arquivos
    pasta_destino = "dados_extraidos"
    
    # Verificar se a pasta de ZIPs existe
    if not os.path.exists(pasta_zips):
        print(f"✗ Erro: Pasta '{pasta_zips}' não encontrada!")
        print(f"  Certifique-se de ter baixado os arquivos primeiro.")
        print()
        return
    
    # Criar pasta de destino se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        print(f"✓ Pasta '{pasta_destino}' criada!")
    else:
        print(f"✓ Usando pasta existente: '{pasta_destino}'")
    
    print()
    
    # Listar arquivos ZIP
    arquivos_zip = [f for f in os.listdir(pasta_zips) if f.endswith('.zip')]
    
    if not arquivos_zip:
        print(f"✗ Nenhum arquivo ZIP encontrado em '{pasta_zips}'")
        print()
        return
    
    print(f"Encontrados {len(arquivos_zip)} arquivo(s) ZIP para descompactar")
    print()
    
    # Descompactar cada arquivo
    sucessos = 0
    total = len(arquivos_zip)
    
    for i, arquivo_zip in enumerate(arquivos_zip, 1):
        print(f"[{i}/{total}] Descompactando: {arquivo_zip}")
        
        # Caminho completo do arquivo ZIP
        caminho_zip = os.path.join(pasta_zips, arquivo_zip)
        
        try:
            # Abrir arquivo ZIP
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                # Extrair todos os arquivos
                zip_ref.extractall(pasta_destino)
            
            print(f"  ✓ Descompactado com sucesso!")
            sucessos += 1
            
        except Exception as e:
            print(f"  ✗ Erro ao descompactar: {e}")
        
        print()
    
    # Resumo final
    print("=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"Arquivos descompactados: {sucessos}/{total}")
    print(f"Localização: {os.path.abspath(pasta_destino)}")
    print()
    
    # Listar arquivos extraídos
    if os.path.exists(pasta_destino):
        arquivos_extraidos = os.listdir(pasta_destino)
        
        if arquivos_extraidos:
            print(f"Total de arquivos extraídos: {len(arquivos_extraidos)}")
            print()
            print("Primeiros 10 arquivos:")
            for arquivo in sorted(arquivos_extraidos)[:10]:
                caminho_completo = os.path.join(pasta_destino, arquivo)
                
                # Verificar se é arquivo (não pasta)
                if os.path.isfile(caminho_completo):
                    tamanho_mb = os.path.getsize(caminho_completo) / (1024 * 1024)
                    print(f"  • {arquivo} ({tamanho_mb:.2f} MB)")
                else:
                    print(f"  • {arquivo}/ (pasta)")
            
            if len(arquivos_extraidos) > 10:
                print(f"  ... e mais {len(arquivos_extraidos) - 10} arquivo(s)")

def detectar_encoding(caminho_arquivo):
    """
    Detecta automaticamente o encoding do arquivo
    
    Args:
        caminho_arquivo: Caminho do arquivo
    
    Returns:
        String com o encoding detectado (ex: 'utf-8', 'latin1')
    """
    # Ler primeiros bytes do arquivo para detectar encoding
    with open(caminho_arquivo, 'rb') as f:
        resultado = chardet.detect(f.read(100000))  # Lê primeiros 100KB
    
    return resultado['encoding']


def detectar_separador(caminho_arquivo, encoding):
    """
    Detecta o separador usado no arquivo (CSV ou TXT)
    
    Args:
        caminho_arquivo: Caminho do arquivo
        encoding: Encoding do arquivo
    
    Returns:
        Caractere separador (';', ',', '|', '\t', etc)
    """
    # Separadores comuns
    separadores_possiveis = [';', ',', '|', '\t']
    
    try:
        # Ler primeira linha
        with open(caminho_arquivo, 'r', encoding=encoding) as f:
            primeira_linha = f.readline()
        
        # Contar quantas vezes cada separador aparece
        contagens = {}
        for sep in separadores_possiveis:
            contagens[sep] = primeira_linha.count(sep)
        
        # Retornar o separador que aparece mais vezes
        separador = max(contagens, key=contagens.get)
        
        # Se nenhum separador foi encontrado, usar ponto-e-vírgula como padrão
        if contagens[separador] == 0:
            return ';'
        
        return separador
        
    except Exception as e:
        print(f"  Erro ao detectar separador: {e}")
        return ';'  # Padrão da ANS


def ler_arquivo_automaticamente(caminho_arquivo):
    """
    Lê arquivo automaticamente detectando formato, encoding e separador
    
    Args:
        caminho_arquivo: Caminho do arquivo
    
    Returns:
        DataFrame do pandas ou None se houver erro
    """
    nome_arquivo = os.path.basename(caminho_arquivo)
    extensao = os.path.splitext(caminho_arquivo)[1].lower()
    
    print(f"  Analisando: {nome_arquivo}")
    
    try:
        # XLSX - Excel
        if extensao == '.xlsx' or extensao == '.xls':
            print(f"    Formato: Excel ({extensao})")
            df = pd.read_excel(caminho_arquivo)
            print(f"    ✓ Lido com sucesso!")
            return df
        
        # CSV ou TXT
        elif extensao == '.csv' or extensao == '.txt':
            print(f"    Formato: Texto ({extensao})")
            
            # Detectar encoding
            encoding = detectar_encoding(caminho_arquivo)
            print(f"    Encoding detectado: {encoding}")
            
            # Detectar separador
            separador = detectar_separador(caminho_arquivo, encoding)
            print(f"    Separador detectado: '{separador}'")
            
            # Tentar ler o arquivo
            try:
                df = pd.read_csv(
                    caminho_arquivo,
                    sep=separador,
                    encoding=encoding,
                    on_bad_lines='skip',  # Pula linhas com problemas
                    low_memory=False
                )
                print(f"    ✓ Lido com sucesso!")
                return df
                
            except Exception as e:
                # Se falhar, tentar com latin1 (encoding comum da ANS)
                print(f"    Tentando com encoding latin1...")
                df = pd.read_csv(
                    caminho_arquivo,
                    sep=separador,
                    encoding='latin1',
                    on_bad_lines='skip',
                    low_memory=False
                )
                print(f"    ✓ Lido com sucesso!")
                return df
        
        else:
            print(f"    ✗ Formato não suportado: {extensao}")
            return None
            
    except Exception as e:
        print(f"    ✗ Erro ao ler arquivo: {e}")
        return None


def normalizar_colunas(df):
    """
    Normaliza os nomes das colunas
    - Remove espaços extras
    - Converte para minúsculas
    - Remove caracteres especiais
    
    Args:
        df: DataFrame
    
    Returns:
        DataFrame com colunas normalizadas
    """
    # Criar cópia para não modificar o original
    df_normalizado = df.copy()
    
    # Normalizar nomes das colunas
    df_normalizado.columns = (
        df_normalizado.columns
        .str.strip()           # Remove espaços nas pontas
        .str.lower()           # Minúsculas
        .str.replace(' ', '_') # Espaços viram underscore
        .str.replace('ã', 'a') # Remove acentos comuns
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
    
    return df_normalizado


def analisar_estrutura(df, nome_arquivo):
    """
    Analisa e mostra a estrutura do DataFrame
    
    Args:
        df: DataFrame
        nome_arquivo: Nome do arquivo para referência
    """
    print(f"\n  Estrutura do arquivo:")
    print(f"    Linhas: {len(df):,}")
    print(f"    Colunas: {len(df.columns)}")
    print(f"\n  Primeiras colunas:")
    
    # Mostrar primeiras 10 colunas
    for i, coluna in enumerate(df.columns[:10], 1):
        tipo = df[coluna].dtype
        print(f"    {i}. {coluna} ({tipo})")
    
    if len(df.columns) > 10:
        print(f"    ... e mais {len(df.columns) - 10} colunas")

def normalizar_arquivos():
    """
    Função principal que normaliza todos os arquivos
    """
    print("=" * 70)
    print("NORMALIZADOR DE ARQUIVOS - DESPESAS/SINISTROS ANS")
    print("=" * 70)
    print()
    
    # Pasta de origem
    pasta_origem = "dados_despesas_sinistros"
    
    # Pasta de destino
    pasta_destino = "dados_normalizados"
    
    # Verificar se pasta de origem existe
    if not os.path.exists(pasta_origem):
        print(f"✗ Erro: Pasta '{pasta_origem}' não encontrada!")
        print(f"  Execute primeiro: python filtrar_despesas_sinistros.py")
        print()
        return
    
    # Criar pasta de destino
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        print(f"✓ Pasta '{pasta_destino}' criada!")
    else:
        print(f"✓ Usando pasta existente: '{pasta_destino}'")
    
    print()
    print("-" * 70)
    print()
    
    # Listar arquivos para normalizar
    arquivos = [
        f for f in os.listdir(pasta_origem)
        if f.endswith(('.csv', '.txt', '.xlsx', '.xls'))
    ]
    
    if not arquivos:
        print("✗ Nenhum arquivo encontrado para normalizar!")
        print()
        return
    
    print(f"Total de arquivos encontrados: {len(arquivos)}")
    print()
    
    # Processar cada arquivo
    processados = 0
    erros = 0
    
    # Dicionário para armazenar metadados
    metadados = []
    
    for i, nome_arquivo in enumerate(arquivos, 1):
        print(f"[{i}/{len(arquivos)}] " + "=" * 55)
        
        caminho_origem = os.path.join(pasta_origem, nome_arquivo)
        
        # Ler arquivo automaticamente
        df = ler_arquivo_automaticamente(caminho_origem)
        
        if df is None:
            erros += 1
            print()
            continue
        
        # Analisar estrutura
        analisar_estrutura(df, nome_arquivo)
        
        # Normalizar colunas
        print(f"\n  Normalizando colunas...")
        df_normalizado = normalizar_colunas(df)
        
        # Gerar nome do arquivo de saída (sempre CSV)
        nome_base = os.path.splitext(nome_arquivo)[0]
        nome_saida = f"{nome_base}_normalizado.csv"
        caminho_saida = os.path.join(pasta_destino, nome_saida)
        
        # Salvar arquivo normalizado
        print(f"  Salvando: {nome_saida}")
        df_normalizado.to_csv(
            caminho_saida,
            index=False,
            sep=';',
            encoding='utf-8'
        )
        
        tamanho_mb = os.path.getsize(caminho_saida) / (1024 * 1024)
        print(f"  ✓ Salvo ({tamanho_mb:.2f} MB)")
        
        # Guardar metadados
        metadados.append({
            'arquivo_original': nome_arquivo,
            'arquivo_normalizado': nome_saida,
            'linhas': len(df),
            'colunas': len(df.columns),
            'tamanho_mb': round(tamanho_mb, 2)
        })
        
        processados += 1
        print()
    
    # Salvar relatório de metadados
    print("=" * 70)
    print("Salvando relatório de metadados...")
    
    df_metadados = pd.DataFrame(metadados)
    caminho_relatorio = os.path.join(pasta_destino, '_RELATORIO_NORMALIZACAO.csv')
    df_metadados.to_csv(caminho_relatorio, index=False, sep=';', encoding='utf-8')
    
    print(f"✓ Relatório salvo: _RELATORIO_NORMALIZACAO.csv")
    print()
    
    # Resumo final
    print("=" * 70)
    print("RESUMO DA NORMALIZAÇÃO")
    print("=" * 70)
    print(f"Total de arquivos encontrados: {len(arquivos)}")
    print(f"Arquivos processados com sucesso: {processados}")
    print(f"Arquivos com erro: {erros}")
    print(f"Localização: {os.path.abspath(pasta_destino)}")
    print()
    
    # Mostrar resumo dos arquivos
    if processados > 0:
        print("Arquivos normalizados:")
        print()
        for meta in metadados:
            print(f"  • {meta['arquivo_normalizado']}")
            print(f"    Linhas: {meta['linhas']:,} | Colunas: {meta['colunas']} | Tamanho: {meta['tamanho_mb']} MB")
            print()


if __name__ == "__main__":
    main()
    descompactar_zips()

    try:
        import chardet
    except ImportError:
        print("=" * 70)
        print("ERRO: Biblioteca 'chardet' não encontrada!")
        print("=" * 70)
        print()
        print("Para instalar, execute:")
        print()
        print("  pip install chardet")
        print()
        print("Depois execute este script novamente.")
        print("=" * 70)
        exit(1)
    
    normalizar_arquivos()
    # filtrar_despesas_sinistros()

# def ler_arquivo(caminho):
#     ext = os.path.splitext(caminho)[1].lower()

#     if ext == ".csv":
#         return pd.read_csv(caminho, sep=";", encoding="latin-1")
#     elif ext == ".txt":
#         return pd.read_csv(caminho, sep="|", encoding="latin-1")
#     elif ext == ".xlsx":
#         return pd.read_excel(caminho)
#     else:
#         raise ValueError(f"Formato não suportado: {ext}")

# for caminho in arquivos:
#     df = ler_arquivo(caminho)
#     print(f"Arquivo lido: {caminho}")
    # print(df.head())

# def filtrar_arquivo(caminho):
#     filtro = tabela_df['DESCRICAO'].str.contains('EVENTOS/SINISTROS', case=False, na=False)
#     despesas_df = tabela_df[filtro]
#     print(despesas_df)