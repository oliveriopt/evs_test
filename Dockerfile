# Base oficial para Flex Templates Python 3.9
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

# Copiamos código y requirements a una carpeta fija
COPY requirements.txt /template/requirements.txt
COPY main.py         /template/main.py

# Instalar deps de Python (aquí no hay nada, pero es correcto igual)
RUN pip install --no-cache-dir -r /template/requirements.txt

# Variables especiales para Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
