#!/bin/bash
ulimit -n 100000
wget https://s3-us-west-2.amazonaws.com/miniprojectcluster/cluster.jar
java -jar cluster.jar 300 100 32 3 25 0 2


