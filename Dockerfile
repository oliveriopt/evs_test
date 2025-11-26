FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

COPY requirements.txt /template/requirements.txt
COPY main.py         /template/main.py

RUN pip install --no-cache-dir -r /template/requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
