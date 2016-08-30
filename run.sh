#!/bin/bash
#
#
rm output.csv

mvn clean package
java  -Xmx4096m -cp ./target/JsonStreamingParser-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.shipeng.JsonStreamingParser.Engine /Users/sxu/projects/JsonStreamingParser/ias.dat
