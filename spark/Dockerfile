FROM jupyter/pyspark-notebook

USER root
RUN echo "jovyan ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/jovyan

WORKDIR /home/jovyan

USER jovyan

ENV SPARK_OPTS="--master local[3] --driver-memory 1g --conf spark.executor.instances=3 --conf spark.executor.cores=1 --conf spark.executor.memory=1g --conf spark.driver.memory=1g"

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
