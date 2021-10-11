#!/bin/sh

# 1. Make a directory named: /activity1/
hdfs dfs -mkdir -p /activity1

# 2. Put the file RandomText.txt into HDFS as the path: /activity1/data/RandomText.txt
 hdfs dfs -put C:\Users\kakin\OneDrive\Desktop\CS_6500_Analytics\cs6500_sp2021_r02_akinola\RandomText.txt /activity1/data/RandomText.txt
# 3. List the contents of the directory /activity1/data/
-rw-r--r--   1 root supergroup      32768 2021-01-26 06:40 /activity1/data/RandomText.txt
# 4. Move the file /activity1/data/RandomText.txt to /activity1/data/NotSoRandomText.txt
 hdfs dfs -mv /activity1/data/RandomText.txt /activity1/data/NotSoRandomText.txt
# 5. Append the file RandomText.txt to the end of the file: /activity1/data/RandomText.txt
hdfs dfs -appendToFile RandomText.txt /activity1/data/NotSoRando
mText.txt
# 6. List the disk space used by the directory /activity1/data/
hdfs dfs -du -h /activity1/data
# 7. Put the file MoreRandomText.txt into HDFS as the path: /activity1/data/MoreRandomText.txt
hdfs dfs -put \MoreRandomText.txt /activity1/data/MoreRandomText
.txt
# 8. Recursively list the contents of the directory /activity1/
 hdfs dfs -ls -R /activity1
# 9. Remove the directory /activity1/ and all files/directories underneath it
hdfs dfs -rm -d /activity1