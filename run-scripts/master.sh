#!/bin/bash
start=`date +"%s"`
scala -classpath "jawn-parser_2.11-0.8.3.jar:sourcecode_2.11-0.1.1.jar:derive_2.11-0.4.3.jar:upickle_2.11-0.4.3.jar:slf4j-simple-1.7.21.jar:slf4j-api-1.7.21.jar:scala-logging_2.11-3.5.0.jar:distributed-sorting_2.11-1.0.jar" -Dlogback.configurationFile=./logback.xml core.Master 5

end=`date +"%s"`
secs=$((end-start))
echo "Total Time:"
printf '%02dh:%02dm:%02ds\n' $(($secs/3600)) $(($secs%3600/60)) $(($secs%60))

