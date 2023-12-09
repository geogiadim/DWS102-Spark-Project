# DWS102-Spark-Project
Spark project for DWS MSc Program's course named "Technologies for Big Data Analytics" (DWS102).


## Spark Deployment
Based on https://hub.docker.com/r/bitnami/spark#environment-variables

Make sure you have `docker` and `docker-compose` installed before following the instructions.
The project is using an .env file to read enviroment variables so make sure to configure it before getting started. 
```sh
cd spark
cp .env.example .env
docker-compose up
```

Available environment variables:
- SPARK_MASTER_WEBUI_PORT: Port for the master web UI (default: 8080).
- SPARK_MASTER_WEBUI_EXPOSED_PORT: The port to which the Spark Master's web UI listens when accessed from a browser. 
- SPARK_WORKER_WEBUI_PORT: Port for the worker web UI (default: 8081).
- SPARK_WORKER_MEMORY: Total amount of memory to allow Spark worker to use on the machine, e.g. 1g, 512m, 256m.
- SPARK_WORKER_CORES: Total number of (CPU) cores to allow Spark worker to use on the machine.
- NUM_OF_WORKERS: Total number of created workers connected to the spark-master of the Apache Spark cluster. 

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

## Scala Project

A mini documentation for scala project.