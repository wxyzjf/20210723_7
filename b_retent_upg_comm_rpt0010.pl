/opt/etl/prd/etl/APP/ADW/B_RETENT_UPG_COMM_RPT/bin> cat b_retent_upg_comm_rpt0010.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


##my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/B_RETENT_UPG_COMM_RPT/bin/master_dev.pl";
require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here

sub runSQLPLUS{
    my $rc = open(SQLPLUS, "| sqlplus /\@${etlvar::TDDSN}");
    ##my $rc = open(SQLPLUS, "| cat > a.sql");
    unless ($rc){
        print "Cound not invoke SQLPLUS command\n";
        return -1;
    }


    print SQLPLUS<<ENDOFINPUT;
        --${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
set echo on
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
set linesize 2000
alter session force parallel query parallel 30;
alter session force parallel dml parallel 30;

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_001A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_001B_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_001C_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_001D_T');

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_001_T');

set define on;
define rpt_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_s_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),1)-1;


--------------------------------------------------------------------------------------------------------
prompt 'Step B_RETENT_UPG_COMM_001A_T : [ Prepaing change subr log within 2 years] ';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001A_T
(
        trx_key
        ,cust_num
        ,subr_num
        ,Image_Date
        ,Acct_num
        ,Action
        ,Orig_Subr_Num
        ,Orig_Cust_Num
        ,ORIG_Acct_NUM
)
select 
        'CHG_SUBR_'||row_number() over (order by image_date,cust_num,subr_num,acct_num,orig_subr_Num)
        ,trx.cust_num
        ,trx.subr_num
        ,trx.Image_Date
        ,' ' as Acct_Num
        ,trx.Action
        ,trx.Orig_Subr_Num
        ,trx.cust_num
        ,' ' as orig_Acct_NUM
from ${etlvar::ADWDB}.NEW_ACTV_LIST trx
where trx.action in ('M')
    and image_date between date '2016-01-01' and &rpt_e_date ;
commit;


prompt 'Step B_RETENT_UPG_COMM_001A_T : [ Prepaing change cust log within 2 years] ';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001A_T
(
        trx_key 
        ,cust_num
        ,subr_num
        ,Image_Date
        ,Acct_num
        ,Action
        ,Orig_Subr_Num
        ,Orig_Cust_Num
        ,Orig_ACCT_Num
)
select   
        'CHG_CUST_'||row_number() over (order by chg_cust_ok.subr_sw_on_date
                                                ,chg_cust_ok.cust_num
                                                ,chg_cust_ok.subr_num
                                                ,chg_cust_ok.acct_num
                                                ,chg_cust_tx.subr_Num)
        ,chg_cust_ok.cust_num 
        ,chg_cust_ok.subr_num
        ,chg_cust_ok.subr_sw_on_date as image_date
        ,' ' as acct_num
        ,'CHG_CUST' as ACTION
        ,chg_cust_tx.subr_num
        ,chg_cust_tx.cust_num
        ,' ' as orig_acct_num
  from  ${etlvar::ADWDB}.subr_info_hist chg_cust_tx
       ,${etlvar::ADWDB}.subr_info_hist chg_cust_ok
where    chg_cust_tx.subr_sw_off_date = chg_cust_ok.subr_sw_on_date 
        and chg_cust_tx.disc_reason_cd in ('77','HO','SAHO','FAMRP')
        and chg_cust_tx.subr_stat_cd in ('TX')
        and chg_cust_tx.subr_sw_off_date = chg_cust_ok.subr_sw_on_date
        and chg_cust_ok.subr_stat_cd = 'OK'
        and chg_cust_tx.subr_stat_cd <> 'OK'
        and chg_cust_ok.subr_sw_on_date between date '2016-01-01' and &rpt_e_date
        and chg_cust_tx.end_date = date '2999-12-31'
        and chg_cust_ok.end_date = date '2999-12-31'
        and chg_cust_tx.subr_num = chg_cust_ok.subr_num
        and chg_cust_tx.cust_num <> chg_cust_ok.cust_num;
commit;


prompt 'Step B_RETENT_UPG_COMM_001B_T : [ Prepaing month end image of profile for tracing back ]';
Insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001B_T
     ï¼ˆ
     ukey
    ,cust_num
    ,subr_num
    ,acct_num
    ,start_date
    ,end_date
    ,subr_sw_on_ts
    ,subr_sw_off_ts
    ,subr_stat_cd
    ,rate_plan_cd
    ,create_ts ï¼‰ 
select 
     h.cust_num||':'||h.subr_num||':'||to_char(&rpt_e_date,'yyyymmdd') as unkey
    ,h.cust_num
    ,h.subr_num
    ,' ' as acct_num
    ,h.start_date
    ,&rpt_e_date as end_date
    ,h.subr_sw_on_ts
    ,h.subr_sw_off_ts
    ,h.subr_stat_cd
    ,h.rate_plan_cd
    ,sysdate
from ${etlvar::ADWDB}.subr_info_hist h
where &rpt_e_date between h.start_date and h.end_date;
commit;




prompt 'Step B_RETENT_UPG_COMM_001C_T : [ Prepaing month end image of profile for tracing back ]';
---- Only work for OK SU PE status  records for month end
----Only map those still active profile back to two years ----

declare    
begin  
    insert into  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001C_T
      (
         UKEY
        ,TRX_KEY
        ,LV
        ,CUST_NUM
        ,SUBR_NUM
        ,ACCT_NUM
        ,START_DATE
        ,END_DATE
        ,TO_CUST_NUM
        ,TO_SUBR_NUM
        ,TO_ACCT_NUM
        ,SUBR_STAT_CD
        ,TRX_DATE
        ,CREATE_TS    
      )select 
         UKEY
        ,UKEY as TRX_KEY
        ,0 as LV
        ,CUST_NUM
        ,SUBR_NUM
        ,ACCT_NUM
        ,START_DATE
        ,END_DATE
        ,CUST_NUM as TO_CUST_NUM
        ,SUBR_NUM as TO_SUBR_NUM
        ,ACCT_NUM as TO_ACCT_NUM
        ,SUBR_STAT_CD
        ,&rpt_e_date as TRX_DATE
        ,CREATE_TS  
      from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001B_T
     where subr_stat_cd in ('OK','PE','SU');
      commit;
      
      
   for lcnt in 1..8 loop
     insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001C_T
      (
         UKEY
        ,TRX_KEY
        ,LV
        ,CUST_NUM
        ,SUBR_NUM
        ,ACCT_NUM
        ,START_DATE
        ,END_DATE
        ,TO_CUST_NUM
        ,TO_SUBR_NUM
        ,TO_ACCT_NUM
        ,TRX_DATE
        ,SUBR_STAT_CD
        ,CREATE_TS    
      )  
      select 
         t.UKEY
        ,max(l.TRX_KEY) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as ORIG_CUST_NUM
        ,lcnt --t.LV
        ,max(l.ORIG_CUST_NUM) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as CUST_NUM   
        ,max(l.ORIG_SUBR_NUM) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as SUBR_NUM
        ,max(l.ORIG_ACCT_NUM) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as ACCT_NUM
        ,date '2999-12-31' as START_DATE
        ,max(l.IMAGE_DATE) keep (dense_rank first order by l.image_date desc,l.trx_key desc) -1  as END_DATE
        ,max(l.CUST_NUM) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as TO_CUST_NUM   
        ,max(l.SUBR_NUM) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as TO_SUBR_NUM
        ,max(l.ACCT_NUM) keep (dense_rank first order by l.image_date desc,l.trx_key desc) as TO_ACCT_NUM
        ,max(l.IMAGE_DATE) keep (dense_rank first order by l.image_date desc,l.trx_key desc)  as TRX_DATE
        ,' ' as SUBR_STAT_CD
        ,sysdate as create_ts
      from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001C_T t  
          ,${etlvar::MIGDB}.B_RETENT_UPG_COMM_001A_T l ---trx_log
       where t.cust_num = l.cust_num
         and t.subr_num = l.subr_num
         and t.acct_num = l.acct_num
         and t.end_date >= l.image_date - 1
         and t.LV = (lcnt -1)
         and l.trx_key not in (
            select ct.trx_key from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001C_T ct
         )
        group by t.ukey ; 
       commit;
   end loop;
   
end;
/


prompt 'Step B_RETENT_UPG_COMM_001D_T : [ Calculate the start date for trace back the profile ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001D_T
      (
         UKEY
        ,TRX_KEY
        ,LV
        ,CUST_NUM
        ,SUBR_NUM
        ,ACCT_NUM
        ,START_DATE
        ,END_DATE
        ,TO_CUST_NUM
        ,TO_SUBR_NUM
        ,TO_ACCT_NUM
        ,TRX_DATE
        ,MTHEND_SUBR_STAT_CD
        ,CREATE_TS
     ) 
select 
         UKEY
        ,TRX_KEY
        ,LV
        ,CUST_NUM
        ,SUBR_NUM
        ,ACCT_NUM
        ,lag(end_date,1,date '1900-01-01') over (partition by UKEY order by end_date) +1 as START_DATE
        ,END_DATE
        ,TO_CUST_NUM
        ,TO_SUBR_NUM
        ,TO_ACCT_NUM
        ,TRX_DATE
        ,SUBR_STAT_CD as MTHEND_SUBR_STAT_CD
        ,CREATE_TS
from         
    ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001C_T;
commit;

prompt 'Step B_RETENT_UPG_COMM_001_T : [ Filiter out unuse case if one cdr change for twice at same day and map the min subr_sw_on_date ]';
---- In case for handling those change acct case for sw on new case .remap the last month end subr_sw_on_date and get the least one---
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T
      (
         UKEY
        ,TRX_KEY
        ,LV
        ,CUST_NUM
        ,SUBR_NUM
        ,ACCT_NUM
        ,START_DATE
        ,END_DATE
        ,TO_CUST_NUM
        ,TO_SUBR_NUM
        ,TO_ACCT_NUM
        ,TRX_DATE
        ,MTHEND_SUBR_STAT_CD
        ,MIN_UKEY_SUBR_SW_ON_DATE
        ,CREATE_TS
     )
select 
         t.UKEY
        ,t.TRX_KEY
        ,t.LV
        ,t.CUST_NUM
        ,t.SUBR_NUM
        ,t.ACCT_NUM
        ,t.START_DATE
        ,t.END_DATE
        ,t.TO_CUST_NUM
        ,t.TO_SUBR_NUM
        ,t.TO_ACCT_NUM
        ,t.TRX_DATE
        ,t.MTHEND_SUBR_STAT_CD
        ,sw.min_subr_sw_on_date as MIN_UKEY_SUBR_SW_ON_DATE
        ,t.CREATE_TS
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001D_T t
left outer join (
   Select d.ukey ,least(min(p.subr_sw_on_date),min(nvl(pl.subr_sw_on_date,date '2999-12-31')))  min_subr_sw_on_date
        --,min(p.subr_sw_on_date) min_subr_sw_on_date
   from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001D_T d 
   left outer join ${etlvar::ADWDB}.subr_info_hist p
   on  d.cust_num = p.cust_num
    and d.subr_num = p.subr_num
    -- and d.end_date between p.start_date and p.end_date
   left outer join prd_adw.subr_info_hist pl
   on  d.cust_num = pl.cust_num
    and d.subr_num = pl.subr_num
    and &rpt_s_date -1 between pl.start_date and pl.end_date
    group by d.ukey
) sw
on t.ukey = sw.ukey
where t.END_DATE >= t.START_DATE;
commit;



quit;
---------------------------------------------------------
commit;
  exit;
ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}


#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl b_cust_info0010.pl adw_b_cust_info_20051010.dir\n");
    exit(1);
}




#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($MASTER_TABLE);
etlvar::genFirstDayOfMonth($etlvar::TXDATE);
my $ret = runSQLPLUS();
my $post = etlvar::postProcess();

exit($ret);

