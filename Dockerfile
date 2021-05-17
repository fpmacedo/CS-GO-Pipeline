FROM apache/airflow:2.0.2
RUN pip install scrapy
RUN pip install apache-airflow[aws]