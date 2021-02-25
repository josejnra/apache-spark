## Linux Configuration
Set up the environment by defining the environment variables in your `~/.bashrc`.
```shell
# easily define which version of spark and python to use 
export PYTHON_FOR_SPARK=~/venvs/python37/bin
export SPARK_2_4_7=~/Frameworks/spark-2.4.7-bin-hadoop2.7
export SPARK_3_0_2=~/Frameworks/spark-3.0.2-bin-hadoop2.7

###############
# SPARK CONFIG
###############
export SPARK_HOME=$SPARK_3_0_2
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=$PYTHON_FOR_SPARK/python

# run pyspark with ipython
export PYSPARK_DRIVER_PYTHON=$PYTHON_FOR_SPARK/ipython

```

## Jupyter Notebook
In the following link is shown two methods of how to use pyspark with jupyter notebook.
[https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes](https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes)
