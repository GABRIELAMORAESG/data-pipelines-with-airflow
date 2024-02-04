# Start from the official Airflow image
FROM apache/airflow:2.6.0

# Install additional dependencies
RUN pip install apache-airflow-providers-amazon
