#!/bin/bash

#################################################################
#     compile-run.ksh
#################################################################
# This script takes 4 parameters:
# Parameter 1 : Name of folder in projects : e.g. OlypmpicMR1
# Parameter 2 : Name of class : OlympicMR2
# Parameter 3 : Input directory in hdfs:  OlympicMR2-in
# Parameter 4 : Output directory in hdfs:  OlympicMR2-out
#
# The script automates the run of the OlympicMR1 or OlympicMR2 map reduce
#
# It uses a separate directory 'deploy' to copy the source and data files
# to avoid any accidental deletion of theese
#
# It performs the following tasks:
# - clean up the deploy directory and copy the current source and data files
# - remove any inputs and outputs from previous runs from HDFS input
# and output directories provided as parameter 3 and 4
# - copy new input files to HDFS input directory
# - compile and run the main class provided by parameter 2
# - copy the Map Reduce outputs from HDFS to the file system

echo
echo #################################################################
echo     compile-run.ksh
echo  ################################################################
echo Check compile-run-$2.log for the log


# Log output to file
exec &> compile-run-$2.log

echo #################################################################
echo     compile-run.ksh
echo  ################################################################
now=$(date)
echo "$now"

echo  Parameter 1 : Name of folder in projects : $1
echo  Parameter 2 : Name of class : $2
echo  Parameter 3 : Input directory in hdfs:  $3
echo  Parameter 4 : Output directory in hdfs:  $4

# Copy the necessary files
echo Go to home directory for hduser

cd ~

now=$(date)
echo "$now"

echo --------------------------------------------
echo Setup the deploy directory
echo --------------------------------------------
echo Clear the deploy directory...
rm ~/deploy/$2/*
rm ~/deploy/$2/data/*
rm ~/deploy/$2/src/*
rm ~/deploy/$2/out/*

cd ~/deploy/$2/
ls
cd ~/deploy/$2/data
ls
cd ~/deploy/$2/src
ls
cd ~/deploy/$2/out
ls

echo Copy sh script to deploy directory
cp ~/projects/$1/scripts/starth.sh ~/deploy/$2/

echo Copy data directory to deploy
cp ~/projects/$1/data/$2/*  ~/deploy/$2/data/

echo Copy src directory to deploy
cp ~/projects/$1/src/*  ~/deploy/$2/src/

cd ~/deploy/$2/

echo Ensure deploy directory has the right permissions
sudo chmod 777 *

echo Make sure hadoop is started
starth.sh

echo Current location...
pwd

now=$(date)
echo "$now"
echo --------------------------------------------
echo Clear directories and files from hdfs...
echo --------------------------------------------

echo Show the content of the ouput directory on hdfs...
hdfs dfs -ls $4

echo Remove content of the ouput directory on hdfs...
hdfs dfs -rm $4/*

echo Remove output directory on hdfs...
hdfs dfs -rmdir $4

echo Show the content of the input directory on hdfs...
hdfs dfs -ls $3

echo Remove all input files on hdfs...
hdfs dfs -rm -r $3/*

echo Show the content of the input directory on hdfs...
hdfs dfs -ls $3

echo Remove input directory on hdfs...
hdfs dfs -rmdir $3

echo Create input directory on hdfs ...
hdfs dfs -mkdir $3

cd ~/deploy/$2/data/

sudo chmod 777 *

now=$(date)
echo "$now"
echo --------------------------------------------
echo Copy data file to the hdfs input directory...
echo --------------------------------------------
hdfs dfs -copyFromLocal *.csv $3
#cd src

echo Show the content of the input directory on hdfs...
hdfs dfs -ls $3

echo Move to the src directory for execution
cd ~/deploy/$2/src/

now=$(date)
echo "$now"
echo --------------------------------------------
echo Execution...
echo --------------------------------------------

echo Compile...
hadoop com.sun.tools.javac.Main *.java

echo Generate JAR...
jar cf $2.jar *.class

echo Execute main java class...
hadoop jar $2.jar $2 $3 $4

hdfs dfs -ls $4

now=$(date)
echo "$now"
echo ----------------------------------------------------
echo Get the output file from hdfs to the file system...
echo -----------------------------------------------------
hdfs dfs -get $4/* ~/deploy/$2/out/

now=$(date)
echo "$now"