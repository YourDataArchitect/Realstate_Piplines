FROM quay.io/astronomer/astro-runtime:12.8.0
# Add include/ to PYTHONPATH so Airflow can import helper code at parse‑time
ENV PYTHONPATH="/usr/local/airflow/include:${PYTHONPATH}"

# Disable all secret redaction patterns becasue it acauses issues with onthemarket pipeline
ENV AIRFLOW__CORE__MASK_SECRETS=False

# Grant permissions to the include directory
USER root
RUN mkdir -p /usr/local/airflow/include/onthemarket_resources/output \
    && chmod -R 777 /usr/local/airflow/include/onthemarket_resources/output \
    && mkdir -p /usr/local/airflow/include/rightmove_recouces/output \
    && chmod -R 777 /usr/local/airflow/include/rightmove_recouces/output
USER astro


