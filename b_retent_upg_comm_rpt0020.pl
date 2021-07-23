/opt/etl/prd/etl/APP/ADW/B_RETENT_UPG_COMM_RPT/bin> cat b_retent_upg_comm_rpt0020.pl
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

------------Use for preparing change plan
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_002A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_003A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004B_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004C_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004D_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004E_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004F_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004G_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_004_T');

set define on;
define rpt_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_s_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),1)-1;

--------##### Praparing change plan case  comparing for current month end and last month case ---------------------------------------------------------------

prompt 'Step B_RETENT_UPG_COMM_002A_T : [ Calculate plan  change case of comparing last and current month end image plan code ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_002A_T
(     case_id
    ,ukey
    ,rpt_mth
    ,cust_num
    ,subr_num
    ,acct_num
    ,mthend_status
    ,rate_plan_cd    
    ,lm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,min_ukey_subr_sw_on_date
    ,create_ts    )
select 
        'TRXCPLAN_'||p.cm_cust_num||'_'||p.cm_subr_num||'_'||to_char(&rpt_e_date,'yymmdd') as case_id
        ,p.ukey
        ,&rpt_s_date as rpt_mth
        ,p.cm_cust_num
        ,p.cm_subr_num
        ,cmp.acct_num
        ,cmp.subr_stat_cd
        ,cmp.rate_plan_cd
        ,lmp.subr_stat_cd
        ,lmp.cust_num
        ,lmp.subr_num
        ,lmp.rate_plan_cd
        ,p.min_ukey_subr_sw_on_date
        ,sysdate as create_ts
from 
    ---- Get profile relationship between  cm and lm mth end
    (Select cm.cust_num as cm_cust_num
            ,cm.subr_num as cm_subr_num
            ,cm.ukey
            ,cm.min_ukey_subr_sw_on_date
            ,lm.cust_num as lm_cust_num
            ,lm.subr_num as lm_subr_num
      from  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T cm
           ,${etlvar::MIGDB}. B_RETENT_UPG_COMM_001_T lm
      where &rpt_e_date between cm.start_date and cm.end_date
       and cm.mthend_subr_stat_cd in ('OK','PE','SU')
       and cm.ukey = lm.ukey
       and  &rpt_s_date -1 between lm.start_date and lm.end_date
   ---Invole New act case 
       --and cm.min_ukey_subr_sw_on_date not between &rpt_s_date and & rpt_e_date
   )p   
   left outer join ${etlvar::ADWDB}.subr_info_hist cmp
        on  p.cm_cust_num = cmp.cust_num
        and p.cm_subr_num = cmp.subr_num
        and &rpt_e_date between cmp.start_date and cmp.end_date
        and cmp.subr_stat_cd in ('OK','PE','SU')
   left outer join ${etlvar::ADWDB}.subr_info_hist lmp
        on p.lm_cust_num = lmp.cust_num
        and p.lm_subr_num = lmp.subr_num
        and &rpt_s_date - 1 between lmp.start_date and lmp.end_date
        and lmp.subr_stat_cd in ('OK','PE','SU')
  where cmp.rate_plan_cd <> lmp.rate_plan_cd;
commit;

              
prompt 'Step B_RETENT_UPG_COMM_003A_T : [ Calculate LD change case of month end image and ld_eff_date within current month]';
---- No LM cust subr  means new active list already consider about change cust_num case.
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_003A_T
(
    case_id
    ,prnk
    ,ukey
    ,rpt_mth
    ,cust_num
    ,subr_num
    ,acct_num
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,lm_cust_num
    ,lm_subr_num
    ,min_ukey_subr_sw_on_date
    ,create_ts
)  
   select  
         'TRXCLD_'||p.cust_num||'_'||p.subr_num||'_'||p.inv_num||'_'||to_char(&rpt_e_date,'yymmdd') as case_id
           ,rank() over (partition by p.cust_num,p.subr_num order by p.ld_exp_date desc ,p.inv_num) prnk
           ,p.ukey
           ,&rpt_s_date as rpt_mth
           ,p.cust_num
           ,p.subr_num
           ,p.acct_num           
           ,p.ld_start_date
           ,p.inv_num
           ,p.ld_cd
           ,p.mkt_cd 
           ,p.ld_exp_date
           ,lm.cust_num
           ,lm.subr_num
           ,p.min_ukey_subr_sw_on_date
           ,sysdate create_ts  
   from (
    ---Month end LD image which ld start within current month 
  select    max(ld.ld_start_date) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as ld_start_date
                ,max(ld.inv_num) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as inv_num
                ,max(ld.ld_cd) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as ld_cd
                ,max(ld.mkt_cd) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as mkt_cd
                ,ld.subr_num 
                ,ld.cust_num 
                ,t.acct_num 
                ,max(ld.start_date) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as start_date
                ,max(ld.end_date) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as end_date
                ,max(ld.ld_expired_date) keep(dense_rank first order by ld.ld_expired_date desc ,ld.ld_start_date desc,hdr.inv_date desc) as ld_exp_date
                ,t.ukey
                ,t.min_ukey_subr_sw_on_date
       from ${etlvar::ADWDB}.subr_ld_hist ld
            ,${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T t
            ,${etlvar::ADWDB}.pos_inv_header hdr
       where &rpt_e_date between ld.start_date and ld.end_date
       and ld.cust_num = t.cust_num
       and ld.subr_num = t.subr_num
       and &rpt_e_date between t.start_date and t.end_date
       and ld.mkt_cd in (
            select mkt_cd from ${etlvar::ADWDB}.mkt_ref_vw m where ld_revenue='P'
       )
       and ld.drdp_flag<> 'Y'
       and ld.void_flg <> 'Y'
       and ld.waived_flg <> 'Y'
       and ld.ld_expired_date > &rpt_e_date
       and ld.ld_start_date between &rpt_s_date and &rpt_e_date
        ---- invole new act case 
       ---and t.min_ukey_subr_sw_on_date not between &rpt_s_date and & rpt_e_date
       and ld.inv_num = hdr.inv_num
  group by ld.subr_num,ld.cust_num,t.acct_num,t.ukey,t.min_ukey_subr_sw_on_date
   ) p left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T lm
   on p.ukey = lm.ukey
   and &rpt_s_date - 1 between lm.start_date and lm.end_date;
commit;

prompt 'Step B_RETENT_UPG_COMM_004A_T : [ Combine the change plan case and change ld case with full join ]';
----- join to main ld ----- so far only one ld allow
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004A_T
(        case_id
        ,sub_case_id_lst
        ,ukey
        ,rpt_mth
        ,cm_cust_num
        ,cm_subr_num
        ,cm_rate_plan_cd        
        ,lm_cust_num
        ,lm_subr_num
        ,lm_rate_plan_cd
        ,ld_start_date
        ,ld_inv_num
        ,ld_cd
        ,ld_mkt_cd
        ,ld_exp_date
        ,min_ukey_subr_sw_on_date
        ,create_ts
        ,prnk
)Select 
        case when cp.case_id is not null and cld.case_id is not null
                then 'CPLANLD'
        when cp.case_id is not null
                then 'CPLAN'
        when cld.case_id is not null
                then 'CLD'
        else 'ERROR'
        end||'_'||nvl(cp.cust_num,cld.cust_num)||'_'||nvl(cp.subr_num,cld.subr_num)||'_'||TO_CHAR(&rpt_mth,'YYMMDD') as case_id
       ,cp.case_id||';'||cld.case_id
       ,nvl(cp.ukey,cld.ukey) as ukey
       ,&rpt_mth as rpt_mth
       ,nvl(cp.cust_num,cld.cust_num) as cm_cust_num
       ,nvl(cp.subr_num,cld.subr_num) as cm_subr_num
       ,nvl(cp.rate_plan_cd,' ') as cm_rate_plan_cd
       ,nvl(cp.lm_cust_num,cld.lm_cust_num) as lm_cust_num
       ,nvl(cp.lm_subr_num,cld.lm_subr_num) as lm_subr_num       
       ,nvl(cp.lm_rate_plan_cd,' ') as lm_rate_plan_cd
       ,nvl(cld.ld_start_date,date '1900-01-01') as cld
       ,nvl(cld.ld_inv_num,' ') as ld_inv_num
       ,nvl(cld.ld_cd,' ') as ld_cd
       ,nvl(cld.ld_mkt_cd,' ') as ld_mkt_cd
       ,nvl(cld.ld_exp_date,date '1900-01-01') as ld_exp_date
       ,nvl(cp.min_ukey_subr_sw_on_date,cld.min_ukey_subr_sw_on_date) as min_ukey_subr_sw_on_date
       ,sysdate
       ,nvl(1,cld.prnk) as prnk
from  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_002A_T cp
full join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_003A_T cld
on
    cp.ukey = cld.ukey
 and cld.prnk = 1;
commit;

prompt 'Step B_RETENT_UPG_COMM_004B_T : [ Map family plan case,tagon need to breakdown to time  ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004B_T
(        case_id
        ,sub_case_id_lst
        ,ukey
        ,rpt_mth
        ,cm_cust_num
        ,cm_subr_num
        ,cm_rate_plan_cd
        ,lm_cust_num
        ,lm_subr_num
        ,lm_rate_plan_cd
        ,ld_start_date
        ,ld_inv_num
        ,ld_cd
        ,ld_mkt_cd
        ,ld_exp_date
        ,min_ukey_subr_sw_on_date
        ,cm_fm_flg
        ,cm_fm_main_subr
        ,cm_fm_main_cust
        ,prnk
        ,create_ts
)select
        t.case_id
        ,t.sub_case_id_lst
        ,t.ukey
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.lm_cust_num
        ,t.lm_subr_num
        ,t.lm_rate_plan_cd
        ,t.ld_start_date
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_exp_date
        ,t.min_ukey_subr_sw_on_date
        ,nvl(fm.f_flg, ' ') as cm_fm_flg
        ,nvl(fm.main_subr_num, ' ') as cm_fm_main_subr
        ,nvl(fm.main_cust_num, ' ') as cm_fm_main_cust
        ,t.prnk
        ,t.create_ts
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004A_T t
left outer join (
    select distinct 'FAMILY_MAIN' f_flg
                    ,mob_cust_num as cust_num
                    ,mob_subr_num as subr_num
                    ,mob_cust_num as main_cust_num
                    ,mob_subr_num as main_subr_num  
    from ${etlvar::ADWDB}.om_tagon_map 
    where &rpt_e_date + 1 - (1/3600/24) between to_date(to_char(start_date,'yyyymmdd')||lpad(start_time,6,'0'),'yyyymmddhh24:mi:ss')
                        and to_date(to_char(end_date,'yyyymmdd')||lpad(end_time,6,'0'),'yyyymmddhh24:mi:ss')
    and tag_on_code ='ADDON_VOICE'
    union all
    select distinct 'FAMILY_SUB' f_flg
                    ,tablet_cust_num as cust_num
                    ,tablet_subr_num as subr_num 
                    ,mob_cust_num as main_cust_num
                    ,mob_subr_num as main_subr_num
    from ${etlvar::ADWDB}.om_tagon_map 
    where &rpt_e_date + 1 - (1/3600/24) between to_date(to_char(start_date,'yyyymmdd')||lpad(start_time,6,'0'),'yyyymmddhh24:mi:ss')
                        and to_date(to_char(end_date,'yyyymmdd')||lpad(end_time,6,'0'),'yyyymmddhh24:mi:ss')
    and tag_on_code ='ADDON_VOICE'
)fm
on t.cm_cust_num = fm.cust_num
and t.cm_subr_num = fm.subr_num
and t.prnk = 1 ;
commit;

-----------------------------------------
--prompt 'Step B_RETENT_UPG_COMM_004C_T : [ Insert back family main sim change then trigger by the sub sim commission ]';
--
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004C_T
(
  case_id,
  sub_case_id_lst,
  rpt_mth,
  fm_sub_cust,
  fm_sub_subr,
  ukey,
  corr_fm_main_cust,
  corr_fm_main_subr,
  create_ts
)
select
                'CFMSUB_'||tm.tablet_cust_num||'_'||tm.tablet_subr_num||'_'||to_char(&rpt_mth,'yymmdd') as case_id
                ,'TRXCFMSU_'||tm.tablet_cust_num||'_'||tm.tablet_subr_num||'_'||to_char(&rpt_e_date,'yymmdd') as  sub_case_id_lst
                ,tm.rpt_mth
                ,tm.tablet_cust_num as patch_fm_sub_cust
                ,tm.tablet_subr_num as patch_fm_sub_subr
                ,tm.p.ukey
                ,tm.cm_cust_num as patch_fm_main_cust
                ,tm.cm_subr_num as patch_fm_main_subr
                ,sysdate
from (Select t.rpt_mth
            ,tg.tablet_subr_num
            ,tg.tablet_cust_num
            ,t.cm_cust_num
            ,t.cm_subr_num
  from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004B_T t
      ,${etlvar::ADWDB}.om_tagon_map tg
 where t.cm_fm_flg ='FAMILY_MAIN'
 and t.cm_cust_num = tg.mob_cust_num
 and t.cm_subr_num = tg.mob_subr_num
 and &rpt_e_date + 1 - (1/3600/24) between to_date(to_char(start_date,'yyyymmdd')||lpad(start_time,6,'0'),'yyyymmddhh24:mi:ss')
                        and to_date(to_char(end_date,'yyyymmdd')||lpad(end_time,6,'0'),'yyyymmddhh24:mi:ss')
 and tg.tag_on_code ='ADDON_VOICE') tm
----Only insert back those family plan sub num doesn't invole yet
left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004B_T ex
    on tm.tablet_subr_num = ex.cm_subr_num
    and tm.tablet_cust_num = ex.cm_cust_num
left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T p
        on tm.tablet_subr_num = p.subr_num
        and tm.tablet_cust_num = p.cust_num
        and &rpt_e_date between p.start_date and p.end_date
where ex.cm_subr_num is null;
commit;

prompt 'Step B_RETENT_UPG_COMM_004B_T : [ Combine the all request ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004B_T
(     case_id
    ,sub_case_id_lst
    ,ukey
    ,rpt_mth
    ,cm_cust_num
    ,cm_subr_num
    ,cm_rate_plan_cd
    ,cm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,create_ts
    ,prnk
    ,min_ukey_subr_sw_on_date)
select t.case_id
       ,t.sub_case_id_lst
       ,t.ukey
       ,t.rpt_mth
       ,t.fm_sub_cust as cm_cust_num
       ,t.fm_sub_subr as cm_subr_num
       ,nvl(cmp.rate_plan_cd,' ') as cm_rate_plan_cd
       ,nvl(cmp.subr_stat_cd,' ') as cm_mthend_status
       ,nvl(lm.cust_num,' ') as lm_cust_num
       ,nvl(lm.subr_num,' ') as lm_subr_num
       ,nvl(lmp.rate_plan_cd, ' ') as lm_rate_plan_cd
       ,date '1900-01-01' as ld_start_date
       ,' ' as ld_inv_num
       ,' ' as ld_cd
       ,' ' ld_mkt_cd
       ,date '1900-01-01' as ld_exp_date
       ,'FAMILY_SUB' as cm_fm_flg
       ,t.corr_fm_main_subr as cm_fm_main_subr
       ,t.corr_fm_main_cust as cm_fm_main_cust
       ,sysdate as create_Ts
       ,1 as prnk
       ,nvl(lm.min_ukey_subr_sw_on_date,date '2999-12-31') as min_ukey_subr_sw_on_date
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004C_T t
left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T lm
 on &rpt_s_date -1 between lm.start_date and lm.end_date
 and t.ukey = lm.ukey
left outer join ${etlvar::ADWDB}.subr_info_hist cmp
 on t.fm_sub_subr = cmp.subr_num
 and t.fm_sub_cust = cmp.cust_num
 and &rpt_e_date between cmp.start_date and cmp.end_date
left outer join ${etlvar::ADWDB}.subr_info_hist lmp
 on lm.subr_num = lmp.subr_num
 and lm.cust_num = lmp.cust_num
 and &rpt_e_date between lmp.start_date and lmp.end_date;
commit;
-----------------------------------------

prompt 'Step B_RETENT_UPG_COMM_004C01_T : [ Insert back relate family plan info ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004C01_T
(
        RPT_MTH
        ,CM_FM_MAIN_SUBR
        ,CM_FM_MAIN_CUST
        ,CM_FM_MAIN_SW_ON_DATE
        ,CM_FM_SUB_ACTV_CNT
        ,FM_INFO
        ,CREATE_TS
) select
        &rpt_mth
        ,tg.mob_subr_num
        ,tg.mob_cust_num
        ,nvl(spm.subr_sw_on_date,date '2999-12-31') as fm_main_sw_on
        ,count(sp.subr_num) as CM_FM_SUB_ACTV_CNT
        ,listagg('FM_SUB_'||tg.tablet_subr_num||':"'                  
                  ||'CUST['||tg.tablet_cust_num||']'
                  ||' STAT['||sp.subr_stat_cd||']'
                  ||' SW_ON_DATE['||to_char(sp.subr_sw_on_date,'yyyy-mm-dd')||']'
                  ||'"' ,',') within group (order by tg.tablet_subr_num) as cm_fm_info        
        ,sysdate
from  prd_adw.om_tagon_map tg
left outer join prd_adw.subr_info_hist  spm
  on tg.mob_subr_num = spm.subr_num
  and tg.mob_cust_num = spm.cust_num
  and spm.subr_stat_cd in ('OK','SU')
  and &rpt_e_date  between spm.start_date and spm.end_date
left outer join prd_adw.subr_info_hist  sp
  on tg.tablet_subr_num = sp.subr_num
  and tg.tablet_cust_num = sp.cust_num
  and sp.subr_stat_cd in ('OK','SU')
  and &rpt_e_date  between sp.start_date and sp.end_date  
where 
     &rpt_e_date + 1 - (1/3600/24) between to_date(to_char(tg.start_date,'yyyymmdd')||lpad(tg.start_time,6,'0'),'yyyymmddhh24:mi:ss')
                and to_date(to_char(tg.end_date,'yyyymmdd')||lpad(tg.end_time,6,'0'),'yyyymmddhh24:mi:ss')
  and tg.tag_on_code ='ADDON_VOICE'
  group by         
        tg.mob_subr_num
        ,tg.mob_cust_num
        ,spm.subr_sw_on_date;
commit;


prompt 'Step B_RETENT_UPG_COMM_004D_T : [ Combine the all request patch all missing value ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004D_T
(case_id
    ,sub_case_id_lst
    ,ukey
    ,rpt_mth
    ,cm_cust_num
    ,cm_subr_num
    ,cm_rate_plan_cd
    ,cm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,min_ukey_subr_sw_on_date
    ,flex_flg
    ,flex_inv
    ,flex_ld_start_date
    ,flex_end_date
    ,prnk
    ,create_ts
)select 
     t.case_id
    ,t.sub_case_id_lst
    ,t.ukey
    ,t.rpt_mth
    ,t.cm_cust_num
    ,t.cm_subr_num
    ,cmp.rate_plan_cd as cm_rate_plan_cd
    ,cmp.subr_stat_cd as cm_mthend_status
    ,t.lm_cust_num
    ,t.lm_subr_num
    ,nvl(lmp.rate_plan_cd,' ') as lm_rate_plan_cd
    ,nvl(cld.ld_start_date ,date '2999-12-31') as ld_start_date
    ,nvl(cld.inv_num,' ') as ld_inv_num
    ,nvl(cld.ld_cd,' ' ) as ld_cd
    ,nvl(cld.mkt_cd,' ') as ld_mkt_cd
    ,nvl(cld.ld_expired_date,date '1900-01-01') as ld_exp_date
    ,t.cm_fm_flg
    ,t.cm_fm_main_subr
    ,t.cm_fm_main_cust
    ,t.min_ukey_subr_sw_on_date
    ,case when fx.invoice_no is not null then 'Y' else 'N' end as flex_flg
    ,nvl(fx.invoice_no , ' ' ) as flex_inv
    ,nvl(fx.ld_start_date,date '2999-12-31') as flex_ld_start_date
    ,nvl(fx.flex_end_date,date '1900-01-01') as flex_end_date
    ,t.prnk
    ,sysdate create_ts
from  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004B_T t
left outer join ${etlvar::ADWDB}.FES_FLEXSWITCH_SUBR_HIST fx
        on t.cm_cust_num = fx.cust_num
        and t.cm_subr_num = fx.subr_num
        and &rpt_e_date between fx.start_date and fx.end_date 
        and &rpt_e_date between fx.ld_start_date and fx.flex_end_date
        and t.prnk=1
left outer join ${etlvar::ADWDB}.subr_info_hist lmp
        on t.lm_cust_num = lmp.cust_num
        and t.lm_subr_num = lmp.subr_num
        and &rpt_s_date -1 between lmp.start_date and lmp.end_date
left outer join ${etlvar::ADWDB}.subr_info_hist cmp
        on t.cm_cust_num = cmp.cust_num
        and t.cm_subr_num = cmp.subr_num
        and &rpt_e_date  between cmp.start_date and cmp.end_date
left outer join (
        select  h.subr_num
        ,h.cust_num
        ,max(h.inv_num) keep (dense_rank first order by h.ld_expired_date desc,h.ld_start_date desc) inv_num
        ,max(h.ld_cd) keep (dense_rank first order by h.ld_expired_date desc,h.ld_start_date desc) ld_cd
        ,max(h.ld_start_date) keep (dense_rank first order by h.ld_expired_date desc,h.ld_start_date desc) ld_start_date
        ,max(h.ld_expired_date) keep (dense_rank first order by h.ld_expired_date desc,h.ld_start_date desc) ld_expired_date
        ,max(h.mkt_cd) keep (dense_rank first order by h.ld_expired_date desc,h.ld_start_date desc) mkt_cd
 from ${etlvar::ADWDB}.subr_ld_hist h
 where h.mkt_cd in (select m.mkt_cd from ${etlvar::ADWDB}.mkt_ref_vw m where ld_revenue='P')
 and h.ld_expired_date >= &rpt_e_date 
----- only get those ld already started plan ld 
 and h.ld_start_date <= &rpt_e_date 
 and &rpt_e_date between h.start_date and h.end_date
 and h.drdp_flag<> 'Y'
 and h.void_flg <> 'Y'
 and h.waived_flg <> 'Y'
 group by
        h.subr_num
       ,h.cust_num
) cld 
        on t.cm_subr_num = cld.subr_num
       and t.cm_cust_num = cld.cust_num ;
commit;
prompt 'Step B_RETENT_UPG_COMM_004E_T :[Prepare override case for family subr ]';
insert into mig_adw.B_RETENT_UPG_COMM_004E_T
(
        case_id
        ,cm_subr_num
        ,cm_cust_num
        ,cm_fm_flg
        ,cm_fm_main_subr
        ,cm_fm_main_cust
        ,fm_overr_ld_inv_num
        ,fm_overr_ld_start_date
        ,fm_overr_ld_cd
        ,fm_overr_ld_mkt_cd
        ,fm_overr_ld_exp_date
        ,fm_overr_rate_plan_cd
)
 Select  
         fs.case_id
        ,fs.cm_subr_num
        ,fs.cm_cust_num
        ,fs.cm_fm_flg
        ,fs.cm_fm_main_subr
        ,fs.cm_fm_main_cust
        ,nvl(max(sl.inv_num)keep(dense_rank first order by sl.ld_expired_date),' ')      as fm_overr_ld_inv_num
        ,nvl(max(sl.ld_start_date)keep(dense_rank first order by sl.ld_expired_date),date '2999-12-31') as fm_overr_ld_start_date
        ,nvl(max(sl.ld_cd)keep(dense_rank first order by sl.ld_expired_date),' ')         as fm_overr_ld_cd
        ,nvl(max(sl.mkt_cd)keep(dense_rank first order by sl.ld_expired_date),' ')        as fm_overr_ld_mkt_cd
        ,nvl(max(sl.ld_expired_date)keep(dense_rank first order by sl.ld_expired_date),date '2999-12-31') as fm_overr_ld_exp_date
        ,nvl(p.rate_plan_cd,' ') as fm_overr_rate_plan_cd 
  from (select * from mig_adw.B_RETENT_UPG_COMM_004D_T where cm_fm_flg = 'FAMILY_SUB' )fs
  left outer join prd_adw.subr_ld_hist sl
    on &rpt_e_date between sl.start_date and sl.end_date
       and &rpt_e_date between sl.ld_start_date and sl.ld_expired_date
       and fs.cm_fm_main_cust = sl.cust_num
       and fs.cm_fm_main_subr = sl.subr_num
       and sl.mkt_cd in (select r.mkt_cd
                        from prd_adw.mkt_ref_vw r where ld_revenue='P' )
 left outer join prd_adw.subr_info_hist p
        on  fs.cm_fm_main_subr = p.subr_num
        and fs.cm_fm_main_cust = p.cust_num
        and &rpt_e_date between p.start_date and p.end_date
 where fs.cm_fm_flg = 'FAMILY_SUB'
    group by fs.case_id
        ,fs.cm_subr_num
        ,fs.cm_cust_num
        ,fs.cm_fm_flg
        ,fs.cm_fm_main_subr
        ,fs.cm_fm_main_cust
        ,p.rate_plan_cd;
commit;

prompt 'Step B_RETENT_UPG_COMM_004F_T : [ Override all family sub number ld info and rate plan]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004F_T
(case_id
    ,sub_case_id_lst
    ,ukey
    ,rpt_mth
    ,cm_cust_num
    ,cm_subr_num
    ,cm_rate_plan_cd
    ,cm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,min_ukey_subr_sw_on_date
    ,flex_flg
    ,flex_inv
    ,flex_ld_start_date
    ,flex_end_date
    ,prnk
    ,calc_rmk_json
    ,cm_fm_info
    ,create_ts
) select
     t.case_id
    ,t.sub_case_id_lst
    ,t.ukey
    ,t.rpt_mth
    ,t.cm_cust_num
    ,t.cm_subr_num
    --,t.cm_rate_plan_cd
    ,decode(t.cm_fm_flg,'FAMILY_SUB',nvl(fmo.fm_overr_rate_plan_cd,' '),t.cm_rate_plan_cd) as cm_rate_plan_cd
    ,t.cm_mthend_status
    ,t.lm_cust_num
    ,t.lm_subr_num
    ,t.lm_rate_plan_cd
    ,decode(t.cm_fm_flg,'FAMILY_SUB',nvl(fmo.fm_overr_ld_start_date,date '2999-12-31'),t.ld_start_date) as ld_start_date
    ,decode(t.cm_fm_flg,'FAMILY_SUB',nvl(fmo.fm_overr_ld_inv_num,' '),t.ld_inv_num) as ld_inv_num
    ,decode(t.cm_fm_flg,'FAMILY_SUB',nvl(fmo.fm_overr_ld_cd,' '),t.ld_cd)  as ld_cd
    ,decode(t.cm_fm_flg,'FAMILY_SUB',nvl(fmo.fm_overr_ld_mkt_cd,' '),t.ld_mkt_cd) as ld_mkt_cd
    ,decode(t.cm_fm_flg,'FAMILY_SUB',nvl(fmo.fm_overr_ld_exp_date,date '1900-01-01'),t.ld_exp_date) as ld_exp_date
    ,t.cm_fm_flg
    ,t.cm_fm_main_subr
    ,t.cm_fm_main_cust
    ,t.min_ukey_subr_sw_on_date
    ,t.flex_flg
    ,t.flex_inv
    ,t.flex_ld_start_date
    ,t.flex_end_date
    ,t.prnk
    , '{'
        ||case when t.cm_fm_flg='FAMILY_SUB' then 'FAMILY_SUB_ORIG_PLAN_CD="'||t.cm_rate_plan_cd||'"'  else '' end 
        ||'}'
        as calc_rmk_json
    ,case when t.cm_fm_flg like 'FAMILY%' then 'CM_FM_SUB_ACTV_CNT:"'||CM_FM_SUB_ACTV_CNT||'",'||fmi.fm_info else ' ' end  as fm_info
    ,t.create_ts
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004D_T t
left outer  join MIG_ADW.B_RETENT_UPG_COMM_004E_T fmo
        on t.case_id = fmo.case_id 
left outer join mig_adw.B_RETENT_UPG_COMM_004C01_T fmi
        on t.cm_fm_main_subr = fmi.cm_fm_main_subr
        and t.cm_fm_main_cust = fmi.cm_fm_main_cust;

commit;

prompt 'Step B_RETENT_UPG_COMM_004G_T : [ Fill the case old ld info ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004G_T
(case_id
    ,sub_case_id_lst
    ,ukey
    ,rpt_mth
    ,cm_cust_num
    ,cm_subr_num
    ,cm_rate_plan_cd
    ,cm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,min_ukey_subr_sw_on_date
    ,flex_flg
    ,flex_inv
    ,flex_ld_start_date
    ,flex_end_date
    ,prnk
    ,calc_rmk_json
    ,cm_fm_info
    ,create_ts
    ,lm_ld_inv_num
    ,lm_ld_cd
    ,lm_ld_exp_date
    ,cm_port_type
)
select
     tm.case_id
    ,tm.sub_case_id_lst
    ,tm.ukey
    ,tm.rpt_mth
    ,tm.cm_cust_num
    ,tm.cm_subr_num
    ,tm.cm_rate_plan_cd
    ,tm.cm_mthend_status
    ,tm.lm_cust_num
    ,tm.lm_subr_num
    ,tm.lm_rate_plan_cd
    ,tm.ld_start_date
    ,tm.ld_inv_num
    ,tm.ld_cd
    ,tm.ld_mkt_cd
    ,tm.ld_exp_date
    ,tm.cm_fm_flg
    ,tm.cm_fm_main_subr
    ,tm.cm_fm_main_cust
    ,tm.min_ukey_subr_sw_on_date
    ,tm.flex_flg
    ,tm.flex_inv
    ,tm.flex_ld_start_date
    ,tm.flex_end_date
    ,tm.prnk
    ,case when lmld_fm.lm_ld_inv_num is not null then 
                replace(tm.calc_rmk_json,'}',',"OVERRIDE_LM_LD"="Y"}')
     else tm.calc_rmk_json
     end calc_rmk_json
    ,tm.cm_fm_info
    ,sysdate as create_ts
    ,nvl(nvl(lmld_fm.lm_ld_inv_num,lmld_normal.lm_ld_inv_num),' ') as lm_ld_inv_num
    ,nvl(nvl(lmld_fm.lm_ld_cd,lmld_normal.lm_ld_cd),' ') as lm_ld_cd
    ,nvl(nvl(lmld_fm.lm_ld_exp_date,lmld_normal.lm_ld_exp_date),date '1900-01-01') as lm_ld_cd
    ,case when p_mnp.cm_port_type is not null then p_mnp.cm_port_type 
          when p_nsp.cm_port_type is not null then p_nsp.cm_port_type 
          when p_mvno.cm_port_type is not null then p_mvno.cm_port_type 
        else ' '
     end cm_port_type
from mig_adw.B_RETENT_UPG_COMM_004F_T tm
------ join last month ld info ----
left outer join 
(
        select t.case_id
      ,nvl(max(lmld.inv_num) keep (dense_rank first order by lmld.ld_start_date desc ),' ') as lm_ld_inv_num
      ,nvl(max(lmld.ld_cd) keep (dense_rank first order by lmld.ld_start_date desc ),' ')  as lm_ld_cd
      ,nvl(max(lmld.ld_expired_date) keep (dense_rank first order by lmld.ld_start_date desc ),date '1900-01-01')  as lm_ld_exp_date
 from mig_adw.B_RETENT_UPG_COMM_004F_T t    
    ,prd_adw.subr_ld_hist lmld
 where t.lm_cust_num = lmld.cust_num
  and t.lm_subr_num = lmld.subr_num  
  and &rpt_s_date -1 between lmld.start_date and lmld.end_date
---remark for some case may end ld at last month end and start new ld at cm
  --and &rpt_s_date -1 between lmld.ld_start_date and lmld.ld_expired_date
  and lmld.mkt_cd  in (select mkt_cd from prd_adw.mkt_ref_vw where ld_revenue ='P')
 group by t.case_id
)lmld_normal
        on tm.case_id = lmld_normal.case_id
------ join last month family case overrideld info ----
left outer join 
(
 select t.case_id
      ,nvl(max(lmld.inv_num) keep (dense_rank first order by lmld.ld_start_date desc ),' ') as lm_ld_inv_num
      ,nvl(max(lmld.ld_cd) keep (dense_rank first order by lmld.ld_start_date desc ),' ')  as lm_ld_cd
      ,nvl(max(lmld.ld_expired_date) keep (dense_rank first order by lmld.ld_start_date desc ),date '1900-01-01')  as lm_ld_exp_date
 from mig_adw.B_RETENT_UPG_COMM_004F_T t
    ,prd_adw.om_tagon_map tg
    ,prd_adw.subr_ld_hist lmld
 where t.lm_cust_num = tg.tablet_cust_num
  and t.lm_subr_num = tg.tablet_subr_num
  and &rpt_s_date - 1 between tg.start_date and tg.end_date
  and tg.mob_subr_num = lmld.subr_num 
  and tg.mob_cust_num = lmld.cust_num
  and &rpt_s_date -1 between lmld.start_date and lmld.end_date
---remark for some case may end ld at last month end and start new ld at cm
---  and &rpt_s_date -1 between lmld.ld_start_date and lmld.ld_expired_date
  and tg.tag_on_code ='ADDON_VOICE'
  and lmld.mkt_cd  in (select mkt_cd from prd_adw.mkt_ref_vw where ld_revenue ='P')
 group by t.case_id
)lmld_fm
        on tm.case_id = lmld_fm.case_id
---- join last month port type info mnp part ---
left outer join 
(
Select t.case_id
        ,'PORT_MNP' cm_port_type
    from mig_adw.B_RETENT_UPG_COMM_004F_T t
                ,prd_adw.subr_info_hist  s
    where t.lm_subr_num = s.subr_num
    and   t.lm_cust_num = s.cust_num
    and &rpt_s_date -1 between s.start_date and s.end_date
    and s.buy_out_num <> ' '
    and t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
)p_mnp
        on tm.case_id = p_mnp.case_id
---- join last month port type info nsp part ---
left outer join 
(
select   t.case_id
        ,'PORT_NSP'  cm_port_type
    from mig_adw.B_RETENT_UPG_COMM_004F_T t
        ,prd_adw.nsp_subr_info_hist nsp
    where t.cm_subr_num = nsp.subr_num
      and &rpt_s_date - 1 between nsp.start_date and nsp.end_date
      and nsp.subr_stat_cd ='OK'
      and t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
)p_nsp
        on tm.case_id = p_nsp.case_id
---- join last month port type info others nsp part ---
left outer join 
(
Select t.case_id
            ,case when max(c.cust_type_cd) in ('TBIRD','MBIRD','ZBIRD') then 'PORT_BIRDE'
                  when max(c.cust_type_cd) in ('GCU2','GCU1') then 'PORT_MVNO'
                  when max(c.cust_type_cd) in ('HKBC2','HKBU2') then 'PORT_HKBN'
            end cm_port_type
    from mig_adw.B_RETENT_UPG_COMM_004F_T t
                ,prd_adw.cust_info_hist  c
    where t.lm_cust_num = c.cust_num
      and &rpt_s_date -1 between c.start_date and c.end_date
      and c.cust_stat_cd ='OK'
      and c.cust_type_cd in ('TBIRD','MBIRD','ZBIRD','GCU2','GCU1','HKBC2','HKBU2')
      and t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
    group by t.case_id
)p_mvno
        on tm.case_id = p_mvno.case_id;
commit;

insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004_T
(case_id
    ,sub_case_id_lst
    ,ukey
    ,rpt_mth
    ,cm_cust_num
    ,cm_subr_num
    ,cm_rate_plan_cd
    ,cm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,min_ukey_subr_sw_on_date
    ,flex_flg
    ,flex_inv
    ,flex_ld_start_date
    ,flex_end_date
    ,prnk
    ,calc_rmk_json
    ,cm_fm_info
    ,create_ts
    ,lm_ld_inv_num
    ,lm_ld_cd
    ,lm_ld_exp_date
    ,cm_port_type
)
select
        case_id
    ,sub_case_id_lst
    ,ukey
    ,rpt_mth
    ,cm_cust_num
    ,cm_subr_num
    ,cm_rate_plan_cd
    ,cm_mthend_status
    ,lm_cust_num
    ,lm_subr_num
    ,lm_rate_plan_cd
    ,ld_start_date
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_exp_date
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,min_ukey_subr_sw_on_date
    ,flex_flg
    ,flex_inv
    ,flex_ld_start_date
    ,flex_end_date
    ,prnk
    ,calc_rmk_json
    ,cm_fm_info
    ,create_ts
    ,lm_ld_inv_num
    ,lm_ld_cd
    ,lm_ld_exp_date
    ,cm_port_type
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004G_T;
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

