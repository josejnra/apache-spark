## Spark Submit

We may use docker images from [Data Mechanics](https://hub.docker.com/r/datamechanics/spark) in order
to run spark apps without the need of installing and configuring it.
All you need to do is to map the source code volume into `/opt/application` and 
change the `command` on the docker-compose file. Then, just `docker-compose up`.
