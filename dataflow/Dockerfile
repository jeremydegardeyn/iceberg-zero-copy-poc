# Flex-template launcher image for BOTH pipelines (build-arg TEMPLATE picks one).
# Custom because beam.managed.Write(ICEBERG) is a cross-language (Java)
# transform: the launcher needs a JVM to run the expansion service, and the
# stock python-template-launcher-base image has none.
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:latest

ARG TEMPLATE
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# requirements.txt is baked into THIS (launcher) image — it only affects graph
# construction. Dataflow WORKERS run Beam's SDK harness container and never see
# it; a Python DoFn's imports must be staged separately via
# FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE. (Cross-language transforms like the
# managed Iceberg sink run in the Java harness, which is why they need neither.)
COPY ${TEMPLATE}/requirements.txt /template/requirements.txt
RUN pip install --no-cache-dir -r /template/requirements.txt
COPY ${TEMPLATE}/worker_requirements.txt /template/worker_requirements.txt
COPY ${TEMPLATE}/main.py /template/main.py

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/worker_requirements.txt
