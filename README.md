# 🏨 Hotel Dataflow BI Platform

**Hotel Dataflow BI Platform** é uma arquitetura moderna de ingestão, processamento e análise de dados para uma **rede hoteleira**, desenvolvida com **Google Cloud Dataflow (Apache Beam - Flex Templates)**.  
O projeto implementa o **modelo medalhão (Bronze → Silver → Gold)**, garantindo governança, versionamento, escalabilidade e qualidade de dados de ponta a ponta — da ingestão bruta até a camada analítica em BigQuery.

---

## 🚀 Visão Geral

A plataforma foi desenhada para operar em ambientes **DataOps e CI/CD**, utilizando **Cloud Build** para automação e **Artifact Registry** + **GCS** como repositórios de imagens e templates do Dataflow.

Cada camada é empacotada em um **container Docker independente**, contendo seu próprio `Dockerfile`, `metadata.json` e pipelines específicos, seguindo as recomendações oficiais do **Google Cloud para Dataflow Flex Templates**.

---

## 🧱 Arquitetura do Repositório

```text
dataflow-pipelines/
├── README.md
├── .gitignore
├── .dockerignore
├── requirements.txt
├── setup.py
│
├── configs/
│ ├── dev/
│ │ ├── bronze.yaml
│ │ ├── silver.yaml
│ │ ├── gold.yaml
│ │ └── common.yaml
│ └── prod/
│ ├── bronze.yaml
│ ├── silver.yaml
│ ├── gold.yaml
│ └── common.yaml
│
├── src/
│ ├── __init__.py
│ ├── common/
│ │ ├── __init__.py
│ │ ├── io_utils.py
│ │ ├── transforms_utils.py
│ │ ├── schema_utils.py
│ │ ├── logging_utils.py
│ │ └── options.py
│ │
│ ├── bronze/
│ │ ├── __init__.py
│ │ ├── pipeline_bronze.py
│ │ ├── transforms_bronze.py
│ │ ├── Dockerfile
│ │ └── metadata.json
│ │
│ ├── silver/
│ │ ├── __init__.py
│ │ ├── pipeline_silver.py
│ │ ├── transforms_silver.py
│ │ ├── Dockerfile
│ │ └── metadata.json
│ │
│ └── gold/
│ ├── __init__.py
│ ├── pipeline_gold.py
│ ├── transforms_gold.py
│ ├── Dockerfile
│ └── metadata.json
│
├── templates/
│ ├── dev/
│ │ ├── bronze_template.json
│ │ ├── silver_template.json
│ │ └── gold_template.json
│ └── prod/
│ ├── bronze_template.json
│ ├── silver_template.json
│ └── gold_template.json
│
├── scripts/
│ ├── build_bronze.sh
│ ├── build_silver.sh
│ ├── build_gold.sh
│ ├── run_bronze_dev.sh
│ ├── run_silver_dev.sh
│ └── run_gold_dev.sh
│
├── tests/
│ ├── __init__.py
│ ├── test_transforms_bronze.py
│ ├── test_transforms_silver.py
│ ├── test_transforms_gold.py
│ └── test_utils.py
│
└── ci/
├── cloudbuild.bronze.yaml
├── cloudbuild.silver.yaml
└── cloudbuild.gold.yaml

---

## 🧩 Descrição das Camadas Medalhão

| Camada  | Conceito | Função no Projeto | Exemplo de Pipeline |
|----------|-----------|------------------|---------------------|
| **Bronze** | Dados brutos (raw) — ingestão sem transformação | Coleta de dados de APIs, Pub/Sub ou arquivos CSV/JSON para o GCS | `pipeline_bronze.py` lê APIs e grava no bucket raw |
| **Silver** | Dados limpos e padronizados | Normaliza schemas, remove duplicidades, aplica regras de negócio e grava no BigQuery (Trusted Zone) | `pipeline_silver.py` lê GCS raw → escreve em BigQuery |
| **Gold** | Dados analíticos e agregados | KPIs, métricas e modelos dimensionais para BI e dashboards | `pipeline_gold.py` lê tabelas Trusted → cria marts e visões |

---

## 📂 Estrutura de Arquivos e Funções

| Caminho / Arquivo | Camada | Descrição / Função | Tipo |
|--------------------|---------|--------------------|------|
| `requirements.txt` | Geral | Dependências globais do projeto | Configuração |
| `setup.py` | Geral | Define o pacote Python e módulos do projeto | Build |
| `.dockerignore` / `.gitignore` | Geral | Exclusão de arquivos do build/versionamento | Sistema |
| `configs/dev/*.yaml` | Dev | Configurações por ambiente (dev, prod) | Parametrização |
| `src/common/io_utils.py` | Comum | Funções genéricas de leitura/escrita (GCS, BQ, API) | Utilitário |
| `src/common/transforms_utils.py` | Comum | Transforms e funções Beam genéricas | Utilitário |
| `src/common/schema_utils.py` | Comum | Schema definition e validação | Utilitário |
| `src/common/logging_utils.py` | Comum | Configuração de logs e Stackdriver | Utilitário |
| `src/common/options.py` | Comum | PipelineOptions customizadas | Core |
| `src/bronze/pipeline_bronze.py` | Bronze | Pipeline principal da camada bronze | Pipeline |
| `src/bronze/transforms_bronze.py` | Bronze | Lógicas de transformação bruta | Transform |
| `src/bronze/Dockerfile` | Bronze | Imagem Flex Template bronze | Infraestrutura |
| `src/bronze/metadata.json` | Bronze | Definição de parâmetros e descrição do template | Metadata |
| `src/silver/pipeline_silver.py` | Silver | Pipeline principal da camada silver | Pipeline |
| `src/silver/transforms_silver.py` | Silver | Limpeza e padronização de dados | Transform |
| `src/silver/Dockerfile` | Silver | Imagem Flex Template silver | Infraestrutura |
| `src/silver/metadata.json` | Silver | Parâmetros de execução da camada silver | Metadata |
| `src/gold/pipeline_gold.py` | Gold | Pipeline principal da camada gold | Pipeline |
| `src/gold/transforms_gold.py` | Gold | Agregações e cálculos de KPIs | Transform |
| `src/gold/Dockerfile` | Gold | Imagem Flex Template gold | Infraestrutura |
| `src/gold/metadata.json` | Gold | Parâmetros e documentação da camada gold | Metadata |
| `scripts/build_*.sh` | Todas | Criação das imagens e templates | Automação |
| `scripts/run_*.sh` | Todas | Execução local ou em ambiente dev | Automação |
| `tests/test_transforms_*.py` | Todas | Testes unitários de cada camada | QA |
| `ci/cloudbuild.*.yaml` | Todas | Pipeline CI/CD no Cloud Build | CI/CD |
| `templates/dev/*.json` | Todas | Templates gerados no GCS (Flex) | Output |

---

## ⚙️ CI/CD (Google Cloud Build)

Cada camada possui seu próprio arquivo de build no diretório `ci/`, permitindo execução independente:

```bash
gcloud builds submit --config ci/cloudbuild.bronze.yaml .
gcloud builds submit --config ci/cloudbuild.silver.yaml .
gcloud builds submit --config ci/cloudbuild.gold.yaml .
O pipeline de CI/CD executa:

Testes unitários (pytest);

Build e push da imagem Docker no Artifact Registry;

Criação do template Flex no bucket GCS correspondente.

☁️ Deploy Manual (sem CI/CD)
bash
gcloud builds submit src/bronze \
  --tag us-central1-docker.pkg.dev/$PROJECT_ID/dataflow/bronze:latest

gcloud dataflow flex-template build gs://$PROJECT_ID-templates/dev/bronze_template.json \
  --image us-central1-docker.pkg.dev/$PROJECT_ID/dataflow/bronze:latest \
  --sdk-language PYTHON \
  --metadata-file src/bronze/metadata.json
Execução:

bash
gcloud dataflow flex-template run "bronze-job-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location gs://$PROJECT_ID-templates/dev/bronze_template.json \
  --region us-central1 \
  --parameters input_url=gs://$PROJECT_ID-raw/input/*.json,output_path=gs://$PROJECT_ID-raw/bronze/out/
🧠 Boas Práticas Adotadas
Arquitetura Medalhão (Bronze/Silver/Gold) para controle de qualidade e rastreabilidade.

Imagens e templates versionados para rollback seguro.

CI/CD desacoplado por camada, facilitando manutenção e evolução.

Parâmetros externos (YAML) para controle de ambientes (dev/prod).

Governança de dados via padronização de schema e logs centralizados.

Testes unitários e integração contínua antes de cada build.

Segurança e isolamento usando Service Accounts e Secrets do GCP.

🧾 Tecnologias Principais
Categoria	Ferramenta / Serviço
Orquestração	Google Dataflow (Apache Beam)
Armazenamento	Google Cloud Storage / BigQuery
CI/CD	Cloud Build + Artifact Registry
Infraestrutura	Docker (Flex Templates)
Observabilidade	Stackdriver Logging
Linguagem	Python 3.9+
Modelo de Dados	Medalhão (Bronze, Silver, Gold)

📜 Licença
Este projeto segue o padrão MIT License (ajustável conforme política da empresa).

👨‍💻 Autor
Michel Santana — Engenheiro de Dados
Projeto desenvolvido como base para uma plataforma analítica unificada de BI hoteleiro, utilizando as melhores práticas de engenharia de dados em GCP.