#!/bin/sh
TODAY=$(date +%Y%m%d)
DATE=$(date +%Y-%m-%d)
TODAY_1D=$(date --date='1 day ago' +%Y%m%d)
TODAY_2D=$(date --date='2 day ago' +%Y%m%d)
/home/hdfs/hdw


pos_trans_dir='/user/hive/warehouse/ods.db/pos_atmtxnjnl/ymd='${TODAY_1D}
sudo -u hdfs hadoop fs -test -e $pos_trans_dir
tmp=$?
if test ! $tmp -eq 0;then
    echo 'pos交易流水表导入失败-------'$tmp  >> logs/logs_dp_failed
    exit -2;
fi

echo '---------------开始清洗pos流水表---------------'
sudo -u hdfs hive <<EOF
use edm;
create table if not exists edm.f_pos_atmtxnjnl_details(
log_no string comment '流水号',
logdat string comment '逻辑日期',
acc_dt string comment '会计日期',
txntim string comment '交易发生时间',
txn_time string comment '交易时间',
txn_cd string comment '交易码',
txn_nm string comment '交易名称',
mercid string comment '商户号',
mertyp string comment '商户类别',
insadr string comment '商户名称 8583报文第43域',
idno string comment '证件号',
ac_no string comment '账号',
crd_no string comment '卡号',
crdflg string comment '卡类型 99 上海分行移动手机支付',
crd_no_bin string comment '卡bin',
card_kind string comment '卡类型代码,C,D...',
card_type string comment '卡类型',
card_name string comment '卡名称',
bank_name string comment '银行名称',
crdno1 string comment '转入卡号',
ccycod string comment '币种',
txnamt string comment '金额',
refamt string comment '已退货金额',
trans_amt bigint comment '交易金额',
fee string comment '手续费，小费，调整金额',
termid string comment '终端号',
terminal_id int comment '终端id',
develop_id int comment '代理商id',
proj_no string comment '收单渠道编码',
proj_name string comment '收单渠道名称',
cpscod string comment '返回码 银联标准应答码',
frsp_cd string comment '前置响应码',
trsp_cd string comment '第三方响应码',
txn_sts string comment '交易状态 S:成功 F:失败 C:被冲正 U:预记状态 X:发送失败 T: 发送超时 E: 其他错误',
txn_typ string comment '交易类型 N:正常交易 C:被冲正交易 D:被撤销交易'
) partitioned by (ymd string);

alter table f_pos_atmtxnjnl_details drop partition (ymd='${TODAY_1D}');
EOF

impala-shell -r <<EOF
insert overwrite table edm.f_pos_atmtxnjnl_details partition (ymd)
select distinct
a.log_no,
a.logdat,
a.acc_dt,
a.txntim,
from_unixtime(unix_timestamp(a.txntim,'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') as txn_time,
a.txn_cd,
c.txn_nm,
a.mercid,
a.mertyp,
a.insadr,
a.idno,
a.ac_no,
a.crd_no,
a.crdflg,
b.accountbin as crd_no_bin,
b.accounttype as card_kind,
b.accountname as card_type,
b.cardname as card_name,
b.bankname as bank_name,
a.crdno1,
a.ccycod,
a.txnamt,
a.refamt,
cast(a.txnamt as int)-cast(a.refamt as int) as trans_amt,
a.fee,
a.termid,
cast(d.terminal_id as int),
cast(d.develop_id as int),
e.proj_no,
e.proj_name,
a.cpscod,
a.frsp_cd,
a.trsp_cd,
a.txn_sts,
a.txn_typ,
a.ymd
from ODS.POS_ATMTXNJNL a
left join (select a0.* from ODS.XSL_CARDBININFO a0
left join (select a3.accountbin
from ODS.XSL_CARDBININFO a3,ODS.XSL_CARDBININFO b3
where substr(a3.accountbin,1,cast(b3.binlen as int))=b3.accountbin and a3.accountlen=b3.accountlen and a3.accountbin<>b3.accountbin) a1 on a0.accountbin=a1.accountbin where a1.accountbin is null ) b on length(a.crd_no) = cast(b.accountlen as int) and substr(a.crd_no,1,cast(b.binlen as int)) = b.accountbin
left join ods.pos_pubtxnpur c on a.txn_cd=c.txn_cd
left join ods.bmcp_t01_cardapp_info d on a.termid=d.term_no
left join ods.bmcp_t02_channel_project_info e on d.card_app_type=e.cardapp_type_id
where a.ymd='${TODAY_1D}';
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME 'edm.f_pos_atmtxnjnl_details的分区'${TODAY_1D}'创建失败--------'$tmp >> logs/logs_dp_failed
fi


pos_trans_dir='/user/hive/warehouse/edm.db/f_pos_atmtxnjnl_details/ymd='${TODAY_1D}
sudo -u hdfs hadoop fs -test -e $pos_trans_dir
tmp=$?
if test ! $tmp -eq 0;then
    echo 'pos交易流水表清洗失败-------'$tmp  >> logs/logs_dp_failed
    exit -2;
fi


echo '---------------开始生成pos交易承兑表---------------'
sudo -u hdfs hive <<EOF
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
use edm;
insert overwrite table edm.f_pos_trans_success partition (ymd)
select * from edm.f_pos_atmtxnjnl_details where txn_cd in ('012001','012006','022001','022006') and txn_typ='N' and txn_sts='S' and ymd='${TODAY_1D}';
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME 'edm.f_pos_trans_success的分区'${TODAY_1D}'创建失败--------'$tmp >> logs/logs_dp_failed
fi


echo '---------------开始合成内部外部pos流水全表---------------'
sudo -u hdfs hive <<EOF
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
use edw;
create table if not exists edw.pos_trans_log_all(
id string comment '主键' ,
orgid string comment '机构号' ,
mer_id string comment '商户号' ,
branch_id string comment '网点号' ,
terminal_id string comment '终端号' ,
log_no string comment '交易流水号' ,
mcc_code string comment '商户MCC' ,
crd_no string comment '交易银行卡号' ,
card_kind string comment '银行卡类型' ,
ccycod string comment '交易币种' ,
trans_amt int comment '交易金额（单位：分）' ,
txn_time string comment '交易时间戳' ,
fee int comment '手续费（单位：分）' ,
txn_sts string comment '交易状态（TXN_STS）' ,
txn_typ string comment '交易类型（TXN_TYP）' ,
txn_cd string comment '交易代码（TXN_CD）' ,
cpscod string comment '应答码' ,
createdate string comment '创建日期' ,
batchid string comment '导入批次号'
) partitioned by (ymd string);

insert overwrite table edw.pos_trans_log_all partition(ymd)
select null as id,
110001 as orgid,
mercid as mer_id,
null  as branch_id,
terminal_id,
log_no,
null  as mcc_code,
crd_no as card_no,
card_kind,
ccycod,
cast(trans_amt as int) as trans_amt,
txn_time as trans_time,
cast(fee as int) as fee,
txn_sts,
txn_typ,
txn_cd,
cpscod,
null as createdate,
null as batchid,
ymd
from edm.f_pos_atmtxnjnl_details
where ymd='${TODAY_1D}';

--插入外部商户流水数据
insert into table edw.pos_trans_log_all partition(ymd)
select id, orgid, 
merchantid as mer_id,
branchid as branch_id,
terminalid as terminal_id,
transactionid as log_no,
merchantmcc as mcc_code,
tradingbankcard as crd_no,
bankcardtype as card_kind,
currency as ccycod,
transactionamount as trans_amt,
transactiontimestamp as trans_time,
counterfee as fee,
tradingstate as txn_sts,
tradingtype as txn_typ,
tradingcode as txn_cd,
cpscode as cpscod,
createdate,
batchid,
from_unixtime(unix_timestamp(transactiontimestamp),'yyyyMMdd') as ymd
from ods1.wbpos_msmertransactionflow
where substr(load_date,1,10)='${DATE}';
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME '内部外部pos流水表edw.pos_trans_log_all的分区'${TODAY_1D}'创建失败--------'$tmp >> logs/logs_dp_failed
fi


echo '---------------开始生成pos交易流水承兑表---------------'
sudo -u hdfs hive <<EOF
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
use edw;
create table if not exists edw.pos_success_trans_log_all like edw.pos_trans_log_all;

insert overwrite table edw.pos_success_trans_log_all partition(ymd)
select * from edw.pos_trans_log_all 
where txn_cd in ('012001','012006','022001','022006','02001') and txn_typ='N' and txn_sts='S';
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME '内部外部pos交易流水承兑表edw.pos_success_trans_log_all的分区'${TODAY_1D}'创建失败--------'$tmp >> logs/logs_dp_failed
fi


echo '---------------开始合成商户信息表---------------'
sudo -u hdfs hive <<EOF
use edw;
create table if not exists edw.pos_merchant_all(
id string comment '主键',
orgid string comment '机构编码',
mer_id string comment '商户号',
merchant_seq string comment '商户序号',
mer_licence_no string comment '商户营业执照号码',
mer_org_licence_no string comment '商户组织机构代码',
tax_registration_no string comment '商户税务登记号',
mer_register_name string comment '商户注册名称',
mer_reg_addr string comment '商户注册地址',
mer_business_name string comment '商户经营名称',
mer_english_name string comment '商户英文名称',
merchant_status string comment '企业状态：在营，停业...',
mer_reg_time string comment '商户注册时间',
mer_in_time string comment '商户进件时间',
registered_capital double comment '商户注册资本（单位：万元）',
corporation_property string comment '企业性质',
mcc_code string comment '商户MCC代码',
business_content string comment '商户实体经营内容',
develop_part string comment '商户发展方类型',
cr_name string comment '商户法人姓名',
cr_licence_type string comment '商户法人证件类型',
cr_licence_no string comment '商户法人证件号码',
cr_mobile string comment '商户法人手机号码',
mer_contact_name string comment '商户联系人姓名',
mer_contact_mobile string comment '商户联系人手机号码',
mer_contact_addr string comment '商户联系地址',
mer_contact_tel string comment '商户联系座机号码',
mer_contact_zipcode string comment '商户联系地址邮编',
mer_contact_email string comment '商户联系email',
partnerno string comment '合作方编号',
createdate string comment '创建日期',
batchid string comment '导入批次号');

insert overwrite table edw.pos_merchant_all
select id,orgid,
a.merchantid as mer_id,
merchantseq as merchant_seq,
businesslicensenum as mer_licence_no,
orgcodecertificate as mer_org_licence_no,
taxregcertificate as tax_registration_no,
registrationname as mer_register_name,
registrationaddr as mer_reg_addr,
merchantname as mer_business_name,
merchantengname as mer_english_name,
merchantstate as merchant_status,
mregisteredtime as mer_reg_time,
merincludedtime as mer_in_time,
mregisteredcapital as registered_capital,
merchanttype as corporation_property,
merchantmcccode as mcc_code,
merentitymanagement as business_content,
mdevparttype as develop_part,
legalpersonname as cr_name,
legalpersondoctype as cr_licence_type,
legalpersonidcard as cr_licence_no,
legalpersontel as cr_mobile,
mercontactname as mer_contact_name,
mercontactmobile as mer_contact_mobile,
mercontactaddr as mer_contact_addr,
mercontacttel as mer_contact_tel,
mercontactzipcode as mer_contact_zipcode,
mercontactemail as mer_contact_email,
partnerno,
createdate,
batchid
from (
    select t.*,row_number() over(partition by merchantid,orgid order by createdate desc) as update_id
    from ods1.wbpos_msmerchant t
) a where a.update_id=1;
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME '内部外部合成商户信息表创建失败--------'$tmp >> logs/logs_dp_failed
fi


echo '---------------开始合成网点信息表---------------'
sudo -u hdfs hive <<EOF
use edw;
create table if not exists edw.pos_merchant_branch_all (
id string comment '主键',
orgid string comment '机构编码',
mer_id string comment '商户号',
branch_id string comment '网点号',
branch_name string comment '网点名称',
branch_english_name string comment '网点英文名称',
branch_state string comment '网点状态',
branch_manage_start_time string comment '网点经营起始时间',
branch_manage_stop_time string comment '网点经营终止时间',
br_businessplace_nature string comment '网点营业场所性质',
branch_store_type string comment '网点店面类型',
br_business_land_area double comment '网点营业用地面积（单位：平米）',
br_business_start_time string comment '网点营业开始时间',
br_business_stop_time string comment '网点营业结束时间',
branch_addr string comment '网点地址',
branch_province_code string comment '网点所在省份代码',
branch_city_code string comment '网点所在城市代码',
branch_area_code string comment '网点所在区域代码',
branch_longitude string comment '网点经度',
branch_latitude string comment '网点纬度',
br_contact_name string comment '网点联系人姓名',
br_contact_mobile string comment '网点联系人手机号码',
br_contact_tel string comment '网点联系座机号码',
br_contact_zipcode string comment '网点联系地址邮编',
br_contact_email string comment '网点联系邮箱',
createdate string comment '创建日期',
batchid string comment '导入批次号');

insert overwrite table edw.pos_merchant_branch_all
select id,orgid,
merchantid as mer_id,
branchid as branch_id,
branchname as branch_name,
branchengname as branch_english_name,
branchstate as branch_state,
brmanagestarttime as branch_manage_start_time,
brmanagestoptime as branch_manage_stop_time,
brbusinessplacenature as br_businessplace_nature,
branchstoretype as branch_store_type,
brbusinesslandarea as br_business_land_area,
brbusinessstarttime as br_business_start_time,
brbusinessstoptime as br_business_stop_time,
branchaddr as branch_addr,
branchprovince as branch_province_code,
branchcity as branch_city_code,
brancharea as branch_area_code,
branchlongitude as branch_longitude,
branchlatitude as branch_latitude,
brcontactname as br_contact_name,
brcontactmobile as br_contact_mobile,
brcontacttel as br_contact_tel,
brcontactzipcode as br_contact_zipcode,
brcontactemail as br_contact_email,
createdate as createdate,
batchid as batchid
from (
    select t.*,row_number() over(partition by branchid,orgid order by createdate desc) as update_id
    from ods1.wbpos_msmerchantbranch t
) a where a.update_id=1;
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME '内部外部合成网点信息表创建失败--------'$tmp >> logs/logs_dp_failed
fi


echo '---------------开始合成终端信息表---------------'
sudo -u hdfs hive <<EOF
use edw;
create table if not exists edw.pos_terminal_info_all (
id string comment '主键',
orgid string comment '组织编码',
mer_id string comment '商户号',
terminal_id string comment '终端编号',
terminal_state string comment '终端状态',
ter_opentime string comment '终端开通时间',
ter_closetime string comment '终端关闭时间',
terminal_mcc string comment '终端MCC',
terminal_property string comment '终端产权',
ter_acceptance_type string comment '终端受理类型',
ter_service_rate double comment '终端服务费率',
discount_standard string comment '终端费率优惠类型',
ter_settle_account string comment '终端结算账号',
ter_settle_accname string comment '终端结算账户名称',
ter_settle_acctype string comment '终端结算账户性质',
ter_settle_period string comment '终端结算周期',
ter_cuplanding_org string comment '终端银联落地机构',
has_trans_limit string comment '是否有交易限额',
per_trans_limit double comment '单笔交易限额（单位：元）',
day_trans_limit double comment '单日交易限额（单位：元）',
ter_comm_method string comment '终端通讯方式',
ter_bind_phone string comment '终端绑定电话',
psam_no string comment '终端PSAM序列号',
createdate string comment '创建日期',
batchid string comment '导入批次号'
);

insert overwrite table edw.pos_terminal_info_all
select id,orgid,
merchantid as mer_id,
terminalid as terminal_id,
terminalstate as terminal_state,
teropentime as ter_opentime,
terclosetime as ter_closetime,
terminalmcc as terminal_mcc,
terminalproperty as terminal_property,
teracceptancetype as ter_acceptance_type,
terservicerate as ter_service_rate,
terratepreferencetype as discount_standard,
tersettlementaccount as ter_settle_account,
tersettlementaccname as ter_settle_accname,
tersettlementacctype as ter_settle_acctype,
tersettlementcycle as ter_settle_period,
tercuplandingorg as ter_cuplanding_org,
istradinglimit as has_trans_limit,
singletradinglimit as per_trans_limit,
onedaytradinglimit as day_trans_limit,
tercommunicationmode as ter_comm_method,
terbindingphone as ter_bind_phone,
terpsam as psam_no,
createdate as createdate,
batchid as batchid
from (
    select t.*,row_number() over(partition by merchantid,orgid order by createdate desc) as update_id
    from ods1.wbpos_msterminalinfo t where terminalid is null
    union all 
    select t.*,row_number() over(partition by terminalid,orgid order by createdate desc) as update_id
    from ods1.wbpos_msterminalinfo t where terminalid is not null
) a where a.update_id=1;
EOF
tmp=$?
NOWTIME=$(date +%Y-%m-%d/%H:%M:%S)
if test ! $tmp -eq 0 ; then
    echo $NOWTIME '内部外部合成终端信息表创建失败--------'$tmp >> logs/logs_dp_failed
fi
