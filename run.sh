#!/bin/bash
###### TEMPLATE run.sh ######
###### YOU NEED TO UNCOMMENT THE FOLLOWING LINE AND INSERT YOUR OWN PARAMETERS ######

spark-submit --conf spark.driver.memory=5g --class PageRank target/project_spark.jar wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph wasb:///pagerank-output

# For more information about tuning the Spark configurations, refer: https://spark.apache.org/docs/2.3.0/configuration.html
