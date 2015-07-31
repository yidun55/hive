1,用"\001"分割
2,字段从左向右分别是：身份证号，性别，出生时间，年龄，发证地点，手机号，
邮箱地址，QQ号，姓名，本金/本息，已还金额，未还/罚息，借款时间，借款期数，公司名称，公司电话，公司地址，居住电话，居住地址，证件地址。
--将贷联盟黑名单数据导入hdfs
hdfs dfs -put /home/dyh/data/credit/person/personMore /data/personMore/personMore.txt;
--创建外部表存放原始贷联盟黑名单爬虫数据

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

create table if not exists spss.c_personmore_increment(
id int,
name string,
caseCode string,
age int,
sexy string,
CardNum string,
courtName string,
areaName string,
partyTypeName int,
gistId string,
regDate string,
gistUnit string,
duty double,
performance string,
disruptTypeName string,
pulishDate string
)
row format delimited fields terminated by '\001';

insert overwrite table spss.c_personmore_increment 
select
cast(id as int),
name string,
caseCode string,
cast(age as int),
sexy string,
CardNum string,
courtName string,
areaName string,
cast(partyTypeName as int),
gistId string,
regDate string,
gistUnit string,
cast(duty as double),
performance string,
disruptTypeName string,
pulishDate string
from spss.c_personmore_increment;

create table spss.c_personmore_increment(
id int,
name varchar2(50),
caseCode varchar2(500),
age int,
sexy varchar2(50),
CardNum varchar2(50),
courtName varchar2(500),
areaName varchar2(50),
partyTypeName int,
gistId varchar2(500),
regDate varchar2(50),
gistUnit varchar2(500),
duty double,
performance varchar2(500),
disruptTypeName varchar2,
pulishDate string
);
