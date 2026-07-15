# Flex-template launcher image for BOTH pipelines (build-arg TEMPLATE picks one).
# Custom because beam.managed.Write(ICEBERG) is a cross-language (Java)
# transform: the launcher needs a JVM to run the expansion service, and the
# stock python-template-launcher-base image has none.
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

ARG TEMPLATE
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY ${TEMPLATE}/requirements.txt /template/requirements.txt
RUN pip install --no-cache-dir -r /template/requirements.txt
COPY ${TEMPLATE}/main.py /template/main.py

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
