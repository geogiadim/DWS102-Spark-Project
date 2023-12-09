# DWS102-Spark-Project
Spark project for DWS MSc Program's course named "Technologies for Big Data Analytics" (DWS102).


## Spark Deployment
Based on https://hub.docker.com/r/bitnami/spark#environment-variables

By default, when you deploy the docker-compose file you will get an Apache Spark cluster with 1 master and 2 workers.
If you want N workers, all you need to do is start the docker-compose deployment with the following command:
```
docker-compose up --scale spark-worker=N
```
