#!/bin/bash

# This script is used to redirect the archives in the urls of the txt file to the EC2 instance


input='/home/ubuntu/weblist.txt'
echo $input
while read url
do

    the_url=""${url}""
    echo $the_url
    echo "Transferring to EC2"
    file_name=$(echo $url | rev | cut -d '/' -f 1 | rev)
    echo $file_name
    wget -qO- $the_url > /home/ubuntu/stackzipdump/$file_name

    #echo "Transfer Complete"
done < "$input"