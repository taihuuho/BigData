#!/bin/bash
args=("$@")
hadoop fs -put customer_data ${args[0]}/customer_data
/usr/bin/spark-submit CustomerAnalyzer.jar ${args[0]}/customer_data ${args[0]}/top_amount_customers ${args[0]}/top_time_elapsed_customers ${args[0]}/min_max_average_time_elapsed_customers