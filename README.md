# DWS102-Spark-Project
Spark project for DWS MSc Program's course named "Technologies for Big Data Analytics" (DWS102).

---
## Spark Docker Deployment
Based on https://hub.docker.com/r/bitnami/spark

Make sure you have `docker` and `docker-compose` installed before following the instructions.
The project is using an .env file to read enviroment variables so make sure to configure it before getting started. 
```sh
cd spark
cp .env.example .env
docker-compose up
```

Available environment variables:
| Variable Name                  | Description                                                                                             | Default Value |
|---------------------------------|---------------------------------------------------------------------------------------------------------|---------------|
| SPARK_MASTER_WEBUI_PORT         | Port for the master web UI                                                                             | 8080          |
| SPARK_MASTER_WEBUI_EXPOSED_PORT  | The port to which the Spark Master's web UI listens when accessed from a browser                        | -             |
| SPARK_WORKER_WEBUI_PORT         | Port for the worker web UI                                                                             | 8081          |
| SPARK_WORKER_MEMORY              | Total amount of memory to allow Spark worker to use on the machine, e.g. 1g, 512m, 256m                  | -             |
| SPARK_WORKER_CORES               | Total number of (CPU) cores to allow Spark worker to use on the machine                                 | -             |
| NUM_OF_WORKERS                  | Total number of created workers connected to the spark-master of the Apache Spark cluster                | -             |


### Spark Shell 
To access Spark Shell inside the spark container run:
```sh
docker-compose exec spark-master sh
```
and then:
```sh
spark-shell
```

### Inspect Spark logs
```sh
docker-compose logs -f spark-master
```

### WebUI for Spark Jobs/Executors
http://localhost:4040/


---
## Scala Project

A mini documentation for scala project.


### Install Scala with cs setup
https://www.scala-lang.org/download/


### Build JAR file 
Run the following command inside scala-project director

```sh
sbt clean assembly
```

You will find the generated JAR file in ```scala-project/target/scala-2.12/```


### Instructions to run the JAR file inside the saprk container

1. Copy datafiles to spark container
    ```sh
    docker cp /your_local_path_to_/datasets spark-master:/opt/bitnami/spark/datafiles
    ```

    Skip this step if you have already mount the dataset direcotry inside docker container

3. Copy JAR file inside container
    ```sh
    docker cp target/scala-2.12/WordCount.jar spark-master:/opt/bitnami/spark
    ```

4. Spark Submit inside container
    ```sh
    spark-submit --master spark://spark-master:7077 WordCount.jar <input_file_path_if_needed>
    ```


!!!Notes
start-master.sh --properties-file /conf/spark-custom.conf
ps aux | grep spark
start-master.sh
start-worker.sh spark://localhost:7077
spark-submit --class WordCount --master spark://localhost:7077 ~./target/scala-2.12/WordCount.jar