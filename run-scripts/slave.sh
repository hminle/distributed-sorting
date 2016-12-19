#!/bin/bash
rm -rfv /data4/490write/*
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
start=`date +"%s"`
scala -classpath "jawn-parser_2.11-0.8.3.jar:sourcecode_2.11-0.1.1.jar:derive_2.11-0.4.3.jar:upickle_2.11-0.4.3.jar:slf4j-simple-1.7.21.jar:slf4j-api-1.7.21.jar:scala-logging_2.11-3.5.0.jar:distributed-sorting_2.11-1.0.jar" core.Slave "10.1.1.46:8517" "/data1/490read/100m/input0.data" "/data1/490read/100m/input1.data" "/data1/490read/100m/input2.data" "/data1/490read/100m/input3.data" "/data1/490read/100m/input4.data" "/data1/490read/100m/input5.data" "/data1/490read/100m/input6.data" "/data1/490read/100m/input7.data" "/data1/490read/100m/input8.data" "/data1/490read/100m/input9.data" "/data4/490write/"

#scala -classpath "jawn-parser_2.11-0.8.3.jar:sourcecode_2.11-0.1.1.jar:derive_2.11-0.4.3.jar:upickle_2.11-0.4.3.jar:slf4j-simple-1.7.21.jar:slf4j-api-1.7.21.jar:scala-logging_2.11-3.5.0.jar:distributed-sorting_2.11-1.0.jar" core.Slave "10.1.1.46:8517" "/data1/490read/100m/input0.data" "/data1/490read/100m/input1.data" "/data1/490read/100m/input2.data" "/data1/490read/100m/input3.data" "/data1/490read/100m/input4.data" "/data1/490read/100m/input5.data" "/data1/490read/100m/input6.data" "/data1/490read/100m/input7.data" "/data1/490read/100m/input8.data" "/data1/490read/100m/input9.data" "/data2/490read/100m/input0.data" "/data2/490read/100m/input1.data" "/data2/490read/100m/input2.data" "/data2/490read/100m/input3.data" "/data2/490read/100m/input4.data" "/data2/490read/100m/input5.data" "/data2/490read/100m/input6.data" "/data2/490read/100m/input7.data" "/data2/490read/100m/input8.data" "/data2/490read/100m/input9.data" "/data3/490read/100m/input0.data" "/data3/490read/100m/input1.data" "/data3/490read/100m/input2.data" "/data3/490read/100m/input3.data" "/data3/490read/100m/input4.data" "/data3/490read/100m/input5.data" "/data3/490read/100m/input6.data" "/data3/490read/100m/input7.data" "/data3/490read/100m/input8.data" "/data3/490read/100m/input9.data" "/data4/490write/"

#scala -classpath "jawn-parser_2.11-0.8.3.jar:sourcecode_2.11-0.1.1.jar:derive_2.11-0.4.3.jar:upickle_2.11-0.4.3.jar:slf4j-simple-1.7.21.jar:slf4j-api-1.7.21.jar:scala-logging_2.11-3.5.0.jar:distributed-sorting_2.11-1.0.jar" core.Slave "10.1.1.46:8517" "/data1/490read/100m/input0.data" "/data1/490read/100m/input1.data" "/data4/490write/"
end=`date +"%s"`
secs=$((end-start))
echo "Total Time:"
printf '%02dh:%02dm:%02ds\n' $(($secs/3600)) $(($secs%3600/60)) $(($secs%60))

