#!/bin/sh


#hdfs dfs -put /home/dyh/data/credit/person/personMore /data/personMore/personMore.txt;


echo '---------------´´½¨Íâ²¿±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_personmore_increment_tmp(
id string,
name string,
caseCode string,
age string,
sexy string,
CardNum string,
courtName string,
areaName string,
partyTypeName string,
gistId string,
regDate string,
gistUnit string,
duty string,
performance string,
disruptTypeName string,
pulishDate string
)
row format delimited fields terminated by '\001'
location '/data/personMore/';
EOF

echo '---------------´´½¨¹ÜÀí±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create table spss.c_personmore_increment as
select
regexp_replace(regexp_replace(cast(unbase64(id) as string),'\r',''),'\n','') as cid,
regexp_replace(regexp_replace(cast(unbase64(name) as string),'\r',''),'\n','') as cname,
regexp_replace(regexp_replace(cast(unbase64(caseCode) as string),'\r',''),'\n','') as caseCode,
regexp_replace(regexp_replace(cast(unbase64(age) as string),'\r',''),'\n','') as age,
regexp_replace(regexp_replace(cast(unbase64(sexy) as string),'\r',''),'\n','') as sexy,
regexp_replace(regexp_replace(cast(unbase64(CardNum) as string),'\r',''),'\n','') as CardNum,
regexp_replace(regexp_replace(cast(unbase64(courtName) as string),'\r',''),'\n','') as courtName,
regexp_replace(regexp_replace(cast(unbase64(areaName) as string),'\r',''),'\n','') as areaName,
regexp_replace(regexp_replace(cast(unbase64(partyTypeName) as string),'\r',''),'\n','') as partyTypeName,
regexp_replace(regexp_replace(cast(unbase64(gistId) as string),'\r',''),'\n','') as gistId,
regexp_replace(regexp_replace(cast(unbase64(regDate) as string),'\r',''),'\n','') as regDate,
regexp_replace(regexp_replace(cast(unbase64(gistUnit) as string),'\r',''),'\n','') as gistUnit,
regexp_replace(regexp_replace(if(length(cast(unbase64(duty) as string))<2000, cast(unbase64(duty) as string), substr(cast(unbase64(duty) as string),0,2000)),'\r',''),'\n','') as duty,
regexp_replace(regexp_replace(cast(unbase64(performance) as string),'\r',''),'\n','') as performance1,
regexp_replace(regexp_replace(if(length(cast(unbase64(disruptTypeName) as string))<2000, cast(unbase64(disruptTypeName) as string), substr(cast(unbase64(disruptTypeName) as string),0,2000)),'\r',''),'\n','') as disruptTypeName,
regexp_replace(regexp_replace(cast(unbase64(pulishDate) as string),'\r',''),'\n','') as pulishDate,
unix_timestamp(regexp_replace(regexp_replace(cast(unbase64(pulishDate) as string),'\r',''),'\n',''),"y年m月d日") as publishDate_timestamp,


from spss.c_personmore_increment_tmp;
EOF
 
# echo '---------------µ¼ÈëÊý¾Ý---------------'
# sudo -u hdfs hive <<EOF
# insert overwrite table spss.c_personmore_increment
# select
# unbase64(id) int,
# unbase64(name) string,
# unbase64(caseCode) string,
# unbase64(age) int,
# unbase64(sexy) string,
# unbase64(CardNum) string,
# unbase64(courtName) string,
# unbase64(areaName) string,
# unbase64(partyTypeName) int,
# unbase64(gistId) string,
# unbase64(regDate) string,
# unbase64(gistUnit) string,
# unbase64(duty) double,
# unbase64(performance) string,
# unbase64(disruptTypeName) string,
# unbase64(pulishDate) string
# from spss.c_personmore_increment_tmp;
# EOF
 
# echo '---------------Éú³Éorcle±í---------------'
# sudo -u hdfs hive <<EOF
# create table spss.c_personmore_increment(
# id int,
# name varchar2(50),
# caseCode varchar2(500),
# age int,
# sexy varchar2(50),
# CardNum varchar2(50),
# courtName varchar2(500),
# areaName varchar2(50),
# partyTypeName int,
# gistId varchar2(500),
# regDate varchar2(50),
# gistUnit varchar2(500),
# duty double,
# performance varchar2(500),
# disruptTypeName varchar2,
# pulishDate string
# );
# EOF

