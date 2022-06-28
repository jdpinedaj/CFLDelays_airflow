# FROM apache/airflow:2.3.2

# # Installing libraries of Python
# COPY requirements.txt /requirements.txt
# RUN pip install --user --upgrade pip
# RUN pip install --no-cache-dir --user -r /requirements.txt

###############################
FROM apache/airflow:2.3.2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y install curl
RUN apt-get install libgomp1

# Changing the default user to airflow to run airflow
USER airflow
# Generating necessary environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV C_FORCE_ROOT="true"

# Installing python libraries
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

# # Creating necessary directories
# RUN mkdir -p ${AIRFLOW_HOME}/dags \
#     && mkdir -p ${AIRFLOW_HOME}/logs \
#     && mkdir -p ${AIRFLOW_HOME}/LibreriasGlobales \
#     && mkdir -p ${AIRFLOW_HOME}/Properties \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/Librerias \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/model \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/data \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/predict \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/scripts
# # Copying necessary files
# COPY ./Scripts/Cargas/Config/Airflow/packages.pth /usr/lib/python3.6/site-packages
#COPY ./Scripts/Cargas/Config/Airflow/airflow_celeryexecutor.cfg ${AIRFLOW_HOME}/airflow.cfg

WORKDIR ${AIRFLOW_HOME}