#!/bin/bash
ulimit -n 100000
wget https://s3-us-west-2.amazonaws.com/miniprojectcluster/cluster.jar
java -jar cluster.jar 100 1000 64 20 25 0 1


