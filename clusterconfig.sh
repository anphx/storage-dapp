#!/bin/bash
ulimit -n 100000
curl -0 https://github.com/an-pham/storage-dapp/raw/master/out/artifacts/cluster/cluster.jar --output cluster.jar
#wget https://s3-us-west-2.amazonaws.com/miniprojectcluster/cluster.jar
java -jar cluster.jar 100 100 64 20 25 0 1


