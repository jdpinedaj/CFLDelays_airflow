# FROM apache/airflow:2.2.3

# # Installing libraries of Python
# COPY requirements.txt /requirements.txt
# RUN pip install --user --upgrade pip
# RUN pip install --no-cache-dir --user -r /requirements.txt

###############################
FROM apache/airflow:2.2.3

#Generar variables de entorno necesarias
ENV AIRFLOW_HOME=/opt/airflow
ENV C_FORCE_ROOT="true"

# Instalar paquetes de python
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

# # Crear los directorios necesarios
# RUN mkdir -p ${AIRFLOW_HOME}/dags \
#     && mkdir -p ${AIRFLOW_HOME}/logs \
#     && mkdir -p ${AIRFLOW_HOME}/LibreriasGlobales \
#     && mkdir -p ${AIRFLOW_HOME}/Properties \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/Librerias \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/model \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/data \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/predict \
#     && mkdir -p ${AIRFLOW_HOME}/Cargas/scripts
# # Copiar los archivos necesarios
# COPY ./Scripts/Cargas/Config/Airflow/packages.pth /usr/lib/python3.6/site-packages
#COPY ./Scripts/Cargas/Config/Airflow/airflow_celeryexecutor.cfg ${AIRFLOW_HOME}/airflow.cfg

WORKDIR ${AIRFLOW_HOME}