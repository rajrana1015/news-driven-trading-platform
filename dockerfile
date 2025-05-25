FROM apache/airflow:2.5.1
USER root
RUN pip install yfinance pandas pyspark transformers
USER airflow
