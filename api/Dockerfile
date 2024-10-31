FROM python:3.9

WORKDIR /app

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

COPY ./ /app

# Install Python dependencies
RUN pip install fastapi uvicorn sqlalchemy psycopg2 pandas requests pyspark

# Set Spark environment variables, using 'spark' as the hostname (matching the service name in docker-compose)
ENV SPARK_MASTER=spark://spark:7077
ENV SPARK_HOME=/usr/local/lib/python3.9/site-packages/pyspark

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

