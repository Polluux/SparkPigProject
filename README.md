# SparkPigProject

# Launch script with spark
$ {spark_directory}/bin/spark-submit spark.py

# Pig
```
export HADOOP_HOME={hadoop_directory}
export PIG_HOME={pig_directory}
export PATH=$PATH:$PIG_HOME/bin
export PIG_CLASSPATH=$HADOOP_HOME/conf
```

$ pig pig.py
