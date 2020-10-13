#shell script to extract stackoverflow xml files

cd /home/ubuntu/talentmap/src/s3;
python3 retrieve_urls.py
./download.sh
./stackzipdump/extract_EC2_s3.sh