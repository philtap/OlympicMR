#!/bin/bash

echo ------------------------------
echo  Starting hadoop
echo ------------------------------

echo Calling start-dfs.sh
start-dfs.sh

echo start-yarn.sh
start-yarn.sh