1,��"\001"�ָ�
2,�ֶδ������ҷֱ��ǣ�����֤�ţ��Ա𣬳���ʱ�䣬���䣬��֤�ص㣬�ֻ��ţ�
�����ַ��QQ�ţ�����������/��Ϣ���ѻ���δ��/��Ϣ�����ʱ�䣬�����������˾���ƣ���˾�绰����˾��ַ����ס�绰����ס��ַ��֤����ַ��
--�������˺��������ݵ���hdfs
hdfs dfs -put /home/dyh/data/credit/person/personMore /data/personMore/personMore.txt;
--�����ⲿ�����ԭʼ�����˺�������������

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