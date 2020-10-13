#!/bin/sh

# To run this file:
# Download 7zip file
#run in the folder stackzipdump


for FILES in *.7z
do
echo $FILES
file_name=$(echo $FILES | cut -d'-' -f 2 | cut -d '.' -f 1 )
echo $file_name
7z e $FILES
echo "extract done"
aws s3 cp $file_name.xml s3://mapthetech/
echo "done $file_name " 

done
