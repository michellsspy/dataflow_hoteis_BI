FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

ENV WORKDIR=/dataflow/template
WORKDIR ${WORKDIR}

# Se usar requirements únicos por camada, copie da camada:
# (ou use um requirements único na raiz, se preferir)
COPY src/bronze/requirements.txt ${WORKDIR}/requirements.txt
COPY src/bronze/setup.py ${WORKDIR}/setup.py
COPY src ${WORKDIR}/src

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/src/bronze/pipeline_bronze.py"

RUN pip install --no-cache-dir -r "${FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE}" \
 && pip install --no-cache-dir -e .

RUN pip install pytest

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
