.PHONY: airflow-up
airflow-up:
	cd airflow && docker-compose --profile pgadmin up -d

airflow-down:
	cd airflow && docker-compose down

airflow-clean:
	cd airflow && docker-compose down --volumes --remove-orphans
	rm -rf db_data/postgres_data/* 


spark-notebook:
	cd spark && docker build -t pyspark-notebook .
	cd spark && docker run --cpus="5" -p 8888:8888 -p 4040-4045:4040-4045 \
		-v ./notebooks:/home/jovyan/notebooks \
 		pyspark-notebook
