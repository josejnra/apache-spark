## Spark 3
First of all you must have installed apache spark on your local machine.
Configure minikube env to build images.
```bash
eval $(minikube -p minikube docker-env)
```

Then, build image based on apache spark dockerfile for python:
```bash
$SPARK_HOME/bin/docker-image-tool.sh -r base-k8s-spark -p $SPARK_HOME/kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
```

Later, build an image of your application. Copying all of your app data to the image.
```bash
docker build -t myspark .
```

In our k8s cluster create a service account and a role
```bash
kubectl create serviceaccount spark
```

```bash
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

Finally, just run the example [script](run.sh). Logs may be found by running:
```bash
kubectl logs spark-driver
```
