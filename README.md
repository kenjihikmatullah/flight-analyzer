### Installation
Configure env, adjust accordingly if needed

```
cp .env.example .env
```

Initialize Docker
```
sudo docker-compose up -d --build
```

Create tables by executing [DDL](./api/data/ddl.sql)
```
docker exec -it <db_container_id> psql -U ${user} -d ${db name}
```


### Usage
Open FastAPI's OpenAPI docs on http://localhost:8000/docs

##### Process Data
This API will let our Python app to invoke Apache Spark to process the data and do the analysis
<img width="1165" alt="process_data" src="https://github.com/user-attachments/assets/a12dcf5f-e48a-4270-b423-3e40931f1119">

##### Get Analysis Results
This API will return the analysis results which are already stored on DB
<img width="1164" alt="get_analysis_results" src="https://github.com/user-attachments/assets/4590d561-554b-4f37-967d-c8cac753df51">

##### Store General Data
Beside, we also store the data in general that might be needed for future analysis
<img width="612" alt="Screenshot 2024-11-01 at 18 06 21" src="https://github.com/user-attachments/assets/c110bca9-4c89-4247-a2be-51f48882d6e9">

### What have done
1. Add functionality to process flight data and conduct simple analysis computing number of delayed flights by leveraging Apache Spark
2. On no (1), also do data cleaning, transformation, and aggregation as needed
3. Expose REST API using FastAPI
4. Store the analysis result as well as general data to a PostgreSQL database
5. Implement overall system using Docker
6. Inject sensitive information via environment variables

### Room for improvement
1. Improve general data that can be stored for future analysis, as well as maintain uniqueness of record
2. Implement dependency injection (DI) for low-level implementation. For example, implement DI on database implementation so that we can easily change from PostgreSQL to another database in the future if needed
3. Improve code cleanliness in general
