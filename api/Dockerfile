FROM python:3.11

WORKDIR /app

# Install Java (required for Spark)
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8
ENV JAVA_HOME /usr/local/openjdk-8
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

COPY ./ /app

# Install Python dependencies
RUN pip install fastapi uvicorn sqlalchemy psycopg2 pandas requests pyspark==3.5.1

# Set Spark environment variables, using 'spark' as the hostname (matching the service name in docker-compose)
ENV SPARK_MASTER=spark://spark:7077

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

