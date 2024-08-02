## Apps availables on this docker-compose file
- Hadoop
- Apache Spark
- Apache Livy
- Jupyter Lab
- Elasticsearch
- Mysql
- Postgres
- Kafka
- Graphite
- Minio

You may simply change the environment variables `HADOOP_VERSION`, `SPARK_VERSION` and `LIVY_VERSION` on the [Dockerfile](Dockerfile) in order to create another docker image with different versions for the services. Besides that, you may have to update the [docker-compose.yaml](docker-compose.yaml) file to be according the new image.

### Build Image
```shell
docker-compose build
```

### Run
```shell
docker-compose up -d
```
Once you run this docker-compose, jupyter lab will be available at this [link](http://localhost:8888). If you want to use apache livy you must uncomment the command that starts the service on [docker-compose.yaml](docker-compose.yaml).

### Execute spark-submit
In order to run your apps, put all your app files within [apps](apps). Then just run:
```shell
./spark-submit.sh example.py
```


### Creates Spark Scala project

```shell
# spark project
sbt new holdenk/sparkProjectTemplate.g8
```

### A few thing to know about

## PyArrow

PyArrow é uma biblioteca em Python que faz parte do projeto Apache Arrow. Ela fornece uma implementação eficiente em memória do formato de dados de coluna Apache Arrow. PyArrow é amplamente usado para a manipulação de dados de alto desempenho, principalmente em grandes volumes de dados, e é uma ferramenta crucial para a interoperabilidade entre diferentes frameworks e sistemas de processamento de dados. 

PyArrow pode ser usado no Apache Spark para melhorar a eficiência de certas operações, especialmente aquelas que envolvem a conversão entre DataFrames do Pandas e DataFrames do Spark. No entanto, o uso de PyArrow não é obrigatório e depende do caso de uso específico e das necessidades de desempenho.

### Quando Usar PyArrow com Spark

- Conversão Rápida entre Pandas e Spark:
    - Se você precisa converter grandes DataFrames do Pandas para Spark DataFrames e vice-versa, habilitar PyArrow pode melhorar significativamente o desempenho dessas operações.
- Processamento em Memória:
    - Para operações que envolvem processamento em memória e onde o PyArrow pode ser mais eficiente do que o mecanismo padrão do Spark.


```python
# enable PyArrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```
