# Imagen base antigua (Python 3 genérico)
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Paquetes de sistema básicos (ODBC libs para pyodbc en el launcher)
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    curl \
    gnupg2 \
    apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Driver Microsoft ODBC 17 para SQL Server (solo en el launcher)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

# Copiamos código y requirements
COPY requirements.txt /template/requirements.txt
COPY main.py         /template/main.py

# Instalar deps de Python en el launcher
RUN pip install --no-cache-dir -r /template/requirements.txt

# Variables especiales para Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
