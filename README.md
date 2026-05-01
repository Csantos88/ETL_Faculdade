# 🏎️ Pipeline ETL: Formula 1 Historical Data (Kaggle ➞ PostgreSQL)

!Apache Airflow
!Python
!Pandas
!PostgreSQL
!Kaggle

## 📌 Visão Geral
Este projeto implementa um pipeline de dados (ETL) automatizado utilizando o **Apache Airflow**. O objetivo principal é extrair dados históricos da Fórmula 1 (1950-2026) diretamente do Kaggle via API, transformá-los usando a biblioteca `pandas` (para unificar informações de corridas, resultados, e pilotos, aplicando filtros temporais parametrizados), e finalmente carregar o resultado consolidado num banco de dados **PostgreSQL**.

O pipeline foi construído com foco em **idempotência**, resiliência a falhas e boas práticas de engenharia de dados, incluindo a limpeza automática de diretórios temporários (*cleanup*).

---

## 📂 Estrutura do Repositório

```text
├── dags/
│   ├── etl_kaggle_postgres_dag.py    # Arquivo Python principal definindo a DAG e lógicas
│   └── sql/
│       └── f1_insert_template.sql    # DDL e TRUNCATE garantindo idempotência
├── data/                             # (Ignorado no git) Armazena arquivos temporários
├── .gitignore                        # Regras de exclusão do git
└── README.md                         # Documentação do projeto
```

---

## ⚙️ Detalhamento das Tasks (Pipeline ETL)

A DAG (`Atividade_ETL_F1_Kaggle`) é composta por quatro tarefas principais, executadas sequencialmente:

### 1. Extração (`extract_task`)
- **Ferramenta:** `PythonOperator` + `KaggleApi`
- **Lógica:** Acessa a API do Kaggle para baixar o dataset de Fórmula 1. As credenciais da API são recuperadas dinamicamente e com segurança através das conexões do Airflow (`kaggle_api`). Após o download, descompacta os arquivos localmente e gera um arquivo mock `status.json`.

### 2. Transformação (`transform_task`)
- **Ferramenta:** `PythonOperator` + `pandas`
- **Lógica:** 
  - **Busca Dinâmica:** Localiza recursivamente arquivos `.csv` necessários (`f1_races`, `f1_results`, `f1_drivers`).
  - **Filtragem por DAG Params:** Filtra os dados de corrida por `start_date` e `end_date`, injetados via contexto do Airflow.
  - **Enriquecimento:** Realiza `Merge` (Inner e Left Joins) unificando corridas, resultados, pilotos e o mock de status do JSON.
  - **Tratamento:** Renomeia colunas para evitar inconsistências e exporta num CSV consolidado limpo.

### 3. Carregamento (`load_task`)
- **Ferramenta:** `PythonOperator` + `PostgresHook`
- **Lógica:** 
  - Executa o template `f1_insert_template.sql` preparando o `f1_schema` e a tabela `results`. Aplica um `TRUNCATE TABLE` prévio.
  - Usa o comando `copy_expert` para aplicar um **Bulk Insert** otimizado (`COPY FROM`), superando a performance de vários inserts convencionais.

### 4. Limpeza (`cleanup_task`)
- **Ferramenta:** `BashOperator`
- **Lógica:** Remove recursivamente via linha de comando (`rm -rf`) todos os dados brutos e arquivos intermediários, poupando disco do Worker e garantindo ambientes isolados por execução.

---

## 🧠 Arquitetura e Decisões de Design

* **Idempotência via TRUNCATE:** A carga sofre `TRUNCATE` antes de cada execução. Isso permite reexecutar backfills sem gerar dados duplicados no DW/Banco.
* **DataFrames na Transformação:** Ideal para este volume histórico de dados; as lógicas ficam isoladas da infraestrutura de banco de dados, sendo calculadas rapidamente em memória pela Task no Worker.
* **Parametrização de Fluxo:** Utilização de `params` na configuração da DAG permite que o usuário filtre o range dos anos da corrida apenas modificando o input visual pela UI no disparo manual (*Trigger w/ config*).
* **Segurança de Credenciais:** Nenhuma chave (`Kaggle Key` ou `DB Password`) trafega pelo código. O Airflow `BaseHook` se encarrega da injeção secreta sob demanda.

---

## 🚀 Como Executar o Projeto Localmente

### Pré-requisitos
- Docker e Docker Compose instalados.
- Conta no Kaggle e Token de API gerado (arquivo `kaggle.json`).

### 1. Clonar o Repositório e Iniciar o Airflow
```bash
# Clone o repositório
git clone [https://github.com/SEU_USUARIO/nome-do-repositorio.git](https://github.com/Csantos88/ETL_Faculdade.git)
cd nome-do-repositorio

# Inicie o ambiente do Airflow usando a imagem oficial do Docker
docker-compose up -d
```

### 2. Configurar Dependências do Python
Certifique-se de que a imagem do Airflow no seu contêiner possui as bibliotecas necessárias. O `pandas`, o pacote oficial do `kaggle` e o provider `postgres` devem estar instalados.

### 3. Configurar Conexões na UI do Airflow
Acesse a UI do Airflow (normalmente em `http://localhost:8080`) e vá em **Admin > Connections**.

**Conexão 1: Kaggle API**
- **Connection Id:** `kaggle_api`
- **Connection Type:** `Generic`
- **Login:** Seu *username* do Kaggle.
- **Password:** Sua *Key/Token* da API do Kaggle.

**Conexão 2: PostgreSQL**
- **Connection Id:** `PG_HL_`
- **Connection Type:** `Postgres`
- **Host / Schema / Login / Password:** Preencha conforme o seu banco de dados de destino.

### 4. Executando o Pipeline
- Na UI do Airflow, ligue o toggle da DAG `Atividade_ETL_F1_Kaggle`.
- Clique em **Play > Trigger DAG w/ config**.
- (Opcional) Edite os parâmetros JSON para escolher as datas desejadas:
  ```json
  {
      "start_date": "2010-01-01",
      "end_date": "2023-12-31"
  }
  ```
- Monitore o log das Tasks na visualização de Grid!
