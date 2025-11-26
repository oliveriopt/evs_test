FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

# Libs nativas necesarias para que pyodbc se pueda importar
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Código y deps Python
COPY requirements.txt /template/requirements.txt
COPY main.py         /template/main.py

RUN pip install --no-cache-dir -r /template/requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"