A sample job using the multi table input format for Apache Accumulo.

To run:

1) execute `mvn clean package`
2) copy the jar in target to $ACCUMULO_HOME/lib/ext
3) run $ACCUMULO_HOME/bin/tool.sh $PATH_TO_JAR mtt.MyJob