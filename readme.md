# IntuitiveCare — Teste de Transformação e Validação de Dados (ANS)

## Visão Geral

Este projeto implementa uma solução completa para **extração, transformação, validação, enriquecimento e agregação** de dados públicos da ANS (Agência Nacional de Saúde Suplementar).

A solução foi desenvolvida em **Python**, organizada em etapas independentes e sequenciais, com foco em:

- robustez no processamento de grandes volumes de dados
- rastreabilidade de inconsistências
- clareza e justificativa das decisões técnicas (trade-offs)

---

## Estrutura do Projeto

```bash
src/
  teste1_pipeline.py         # Teste 1 — pipeline completo (download → consolidação)
  teste2_1_validacao.py      # Teste 2.1 — validação de dados
  teste2_2_enriquecimento.py # Teste 2.2 — enriquecimento com cadastro ANS
  teste2_3_agregacao.py      # Teste 2.3 — agregações estatísticas

dados_ans/                  # ZIPs baixados da ANS (gerado)
dados_extraidos/            # Arquivos extraídos (gerado)
dados_despesas_sinistros/   # Linhas filtradas de despesas (gerado)
dados_normalizados/         # CSVs normalizados (gerado)
dados_consolidados/         # Consolidado final do Teste 1 (gerado)

dados_validados/            # Saída do Teste 2.1 (gerado)
dados_enriquecidos/         # Saída do Teste 2.2 (gerado)
dados_agregados/            # Saída do Teste 2.3 + ZIP final (gerado)
```
- As pastas de dados são criadas automaticamente durante a execução dos scripts.

---

## Requisitos

- Python 3.10+
- Bibliotecas:
    - pandas
    - wget
    - chardet
    - openpyxl

Instalação:

```bash
pip install pandas 
pip install wget 
pip install chardet
pip install openpyxl
```

---

# TESTE 1 — Pipeline de Extração e Consolidação

## Objetivo

1. Baixar os arquivos ZIP trimestrais da ANS
2. Descompactar os arquivos
3. Filtrar linhas relacionadas a despesas assistenciais / eventos / sinistros
4. Normalizar colunas e formatos
5. Consolidar os dados em um único CSV e ZIP

## Execução

```bash
python src/teste1_pipeline.py
```

---

## Principais decisões técnicas — Teste 1

### 🔹 Identificação de despesas assistenciais / eventos / sinistros

A filtragem é realizada **em nível de linha**, utilizando:
- padrões no código contábil
- busca textual em colunas de descrição
- expressões regulares (re) para identificar informações no nome do arquivo
Isso garante que apenas ***despesas assistenciais (eventos/sinistros)** sejam processadas, conforme o Manual Contábil da ANS.

---

### 🔹 Extração de ano e trimestre

O ano e o trimestre são extraídos do nome do arquivo utilizando expressões regulares, por exemplo:

```python
re.search(r"([1-4])T(\d{4})", nome_arquivo)
```

Essa abordagem evita dependência de campos de data inconsistentes dentro dos arquivos e garante consistência temporal entre os dados.

---

### 🔹 Tratamento de valores inválidos

- valores não numéricos são convertidos com ```to_numeric```
- valores nulos, zerados ou negativos não são considerados despesas válidas
- apenas valores positivos entram no consolidado final

---

### 🔹 Trade-off Técnico — Processamento em Memória vs. Incremental

Durante a leitura dos arquivos contábeis da ANS, foi necessário decidir entre:

#### Opção 1 — Processar todo o arquivo em memória
- Leitura completa com ```pd.read_csv```
- Código mais simples e rápido para arquivos pequenos

#### Vantagens:
- menor complexidade
- melhor performance em volumes reduzidos

#### Desvantagens:
- alto consumo de memória
- risco de falha para arquivos grandes
- menor escalabilidade

---

#### Opção 2 — Processamento incremental (em chunks)
- Leitura por blocos (chunksize)
- Processamento sequencial

#### Vantagens:
- menor uso de memória
- maior robustez
- adequado para arquivos grandes

#### Desvantagens:
- código mais verboso
- leve impacto de performance

---

#### ✔️ Decisão adotada
Foi adotado **processamento incremental**, controlado por configuração:

```python
PROCESSAR_INCREMENTAL = True
CHUNK_SIZE = 50000
```

Essa abordagem foi escolhida considerando o volume potencialmente elevado dos arquivos trimestrais da ANS e a necessidade de garantir estabilidade da execução, mesmo em ambientes com recursos limitados.

A implementação permite alternar facilmente para processamento completo caso o volume de dados seja menor.

---

# TESTE 2 — Transformação e Validação
Os testes da Parte 2 são executados de forma sequencial, porém independente.

---

## 2.1 — Validação de Dados

### Objetivo
Validar o consolidado gerado no Teste 1, aplicando regras como:
- valores de despesas positivos
- ano dentro de faixa esperada
- trimestre válido
- campos obrigatórios presentes

### Execução

```bash
python src/teste2_1_validacao.py
```

### Trade-off — Tratamento de registros inválidos
**Decisão adotada:** separar registros inválidos ao invés de corrigi-los automaticamente.

#### Justificativa:
- evita suposições implícitas
- garante rastreabilidade
- facilita auditoria

Saídas:
```bash
dados_validados/despesas_validadas.csv
dados_validados/despesas_invalidas.csv
```

---

## 2.2 — Enriquecimento com Cadastro da ANS

### Objetivo
- Baixar o cadastro de operadoras ativas (Relatorio_cadop.csv)
- Enriquecer o consolidado com:
    - CNPJ
    - Razão Social
    - Modalidade
    - UF
- Tratar inconsistências cadastrais

### Execução
```bash
python src/teste2_2_enriquecimento.py
```

---

### Trade-offs e decisões — Teste 2.2

#### 🔹 Chave de integração (CNPJ)
O consolidado contém reg_ans, enquanto o requisito exige integração por CNPJ.

Decisão:
- obter o CNPJ a partir do cadastro usando reg_ans
- realizar o join final utilizando o CNPJ como chave

#### 🔹 Registros sem correspondência no cadastro
Decisão: manter os registros e marcar explicitamente.
- LEFT JOIN do consolidado com o cadastro
- criação da coluna status_match:   
    - OK
    - SEM_MATCH_NO_CADASTRO
    - SEM_CNPJ_NO_CONSOLIDADO

#### Justificativa:
- evita perda de dados financeiros
- permite análise posterior das inconsistências

#### 🔹 CNPJs duplicados no cadastro
Decisão: agregar o cadastro antes do join.
- agrupamento por CNPJ
- escolha do valor mais frequente por campo
- marcação de divergências com status_cadastro = CADASTRO_DUPLICADO
Essa abordagem evita explosão de linhas e mantém determinismo.

---

## 2.3 — Agregação com Múltiplas Estratégias

### Objetivo
- Agrupar dados por Razão Social e UF
- Calcular:
    - total de despesas
    - média trimestral
    - desvio padrão
    - Ordenar por valor total (decrescente)
    - Gerar CSV final e ZIP

### Execução

```bash
python src/teste2_3_agregacao.py
```

---

### Métricas calculadas

| Métrica              | Descrição                |
| -------------------- | ------------------------ |
| total_despesas       | Soma total das despesas  |
| media_trimestral     | Média das despesas       |
| desvio_padrao        | Variabilidade dos gastos |
| quantidade_registros | Número de registros      |

---

### Trade-off Técnico — Estratégia de Ordenação
Decisão: ordenar os dados após a agregação, e não antes.

#### Justificativa:

- reduz significativamente o volume de dados a ordenar
- menor custo computacional
- abordagem mais eficiente e adequada ao contexto do problema

Saídas:

```bash
dados_agregados/despesas_agregadas.csv
dados_agregados/Teste_Roberta_Moreira.zip
```

---

## Execução Completa (Passo a Passo)

```bash
python src/teste1_pipeline.py
python src/teste2_1_validacao.py
python src/teste2_2_enriquecimento.py
python src/teste2_3_agregacao.py
```

---

## Considerações Finais
A solução prioriza:
- clareza de código
- decisões técnicas explícitas
- rastreabilidade de inconsistências
- robustez no processamento
Todas as etapas foram implementadas considerando boas práticas de engenharia de dados e alinhamento com os requisitos do desafio.