FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

ENV WORKDIR=/dataflow/template
WORKDIR ${WORKDIR}

# Copia apenas o necessário
COPY requirements.txt ${WORKDIR}/requirements.txt
COPY setup.py ${WORKDIR}/setup.py
COPY src ${WORKDIR}/src

# Instala dependências
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/src/silver/pipeline_silver.py"

RUN pip install --no-cache-dir -r "${FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE}" \
 && pip install --no-cache-dir -e .

# (Opcional) outras variáveis:
# ENV FLEX_TEMPLATE_PYTHON_PY_OPTIONS=""

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
