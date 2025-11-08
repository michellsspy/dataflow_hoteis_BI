FROM apache/beam_python3.10_sdk:2.55.0

# Define o diretório de trabalho
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# Copia o código do template
COPY . .

# Instala as dependências do pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt

# Variáveis de ambiente obrigatórias
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=30
ENV FLEX_TEMPLATES_NUM_LOG_LINES=1000

# Usa o launcher oficial do Beam (já incluído)
ENTRYPOINT ["/opt/apache/beam/boot"]
