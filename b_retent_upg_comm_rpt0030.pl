/opt/etl/prd/etl/APP/ADW/B_RETENT_UPG_COMM_RPT/bin> cat b_retent_upg_comm_rpt0030.pl
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
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005B_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005C_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005D_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005E_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005F_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005G_T');
--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005H_T');
--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_005_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_RAW_UAT');

set define on;
define rpt_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_s_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),1)-1;

--------##### Praparing change plan case  comparing for current month end and last month case ---------------------------------------------------------------
prompt 'Step B_RETENT_UPG_COMM_005A_T : [ Prepare GZ call centre flag and profile ]';
--------##### Get gz call centre log for latest half year

insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005A_T
(
        Cust_Num
        ,Subr_Num
        ,Gz_Call_Centre_Flg
        ,program_id
        ,comm_date
        ,comm_user
        ,full_name
        ,Last_Contact_Date
        ,create_ts
)
SELECT 
        cc.Cust_Num
        ,cc.Subr_Num
        ,'Y' AS GZ_Call_Centre_Flg
        ,cc.PROGRAM_ID
        ,cc.comm_date
        ,to_char(cc.comm_user)
        ,f.full_name       
        ,cc.Last_Contact_Date        
        ,sysdate
FROM   prd_adw.PRO_cold_call cc
left outer join prd_adw.fes_usr_info f
        on cc.comm_user = f.fes_usr_id
      ,prd_adw.pro_program_info  pro    
    where cc.program_id = pro.program_id
    and cc.comm_date >= &rpt_s_date -180 
    --and pro.end_date >=  &rpt_s_date
    --and pro.start_date < &rpt_e_date
    and substr(cc.Program_ID,1,4) IN ('COLD')
    -- exclude sysadmin--
    and comm_user <> '1'
    AND cc.Status IN ('AO')
union all
select 
     s.cust_num
    ,s.subr_num
    ,'Y' GZ_Call_Centre_flg
    ,p.program_id
    ,s.comm_date
    ,s.comm_by as comm_user
    ,s.comm_by as full_name
    ,s.last_contact_date    
    ,sysdate
from prd_adw.pr_status s
    ,prd_adw.pro_program_info p
where s.comm_date>= &rpt_s_date -180
and s.pos_status ='Success'
and s.program_id = p.program_id
--and p.end_date >= &rpt_s_date
--and p.start_date < &rpt_e_date
and substr(p.program_id,1,3) IN ('PRR','PRS','PR0');
commit;
     
---- get current month bill_cd earilest start date within current month
---- get last month bill_cd latest end date date within current month 
prompt 'Step B_RETENT_UPG_COMM_005B01_T : [ Mapping the additional info ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005B01_T
(
     rpt_mth
    ,case_id
    ,cm_subr_num
    ,cm_cust_num
    ,lm_subr_num
    ,lm_cust_num
    ,ld_inv_num
    ,ld_start_date
    ,cm_rate_plan_cd
    ,cm_bill_start_date
    ,cm_bill_store_cd
    ,cm_bill_salesman_cd
    ,cm_ld_store_cd
    ,cm_ld_salesman_cd
    ,online_inv_flg
    ,lm_bill_end_date
    ,gz_call_centre_flg
    ,last_contact_date
    ,cm_cc_salesman_cd
    ,cm_cc_store_cd
    ,cm_cc_comm_date
    ,ld_inv_date
)select
     t.rpt_mth
     ,t.case_id
     ,max(t.cm_subr_num)
     ,max(t.cm_cust_num)
     ,max(t.lm_subr_num)
     ,max(t.lm_cust_num)
     ,max(t.ld_inv_num)
     ,max(t.ld_start_date)
     ,max(t.cm_rate_plan_cd)
     ,nvl(min(cmbs.bill_start_date) keep (dense_rank first order by cmbs.bill_start_date desc ),date '1900-01-01') as cm_bill_start_date  
     ,nvl(min(cmuf.pos_shop_cd) keep (dense_rank first order by cmbs.bill_start_date desc),' ') as cm_bill_store_cd
     ,nvl(min(cmbs.salesman_cd) keep (dense_rank first order by cmbs.bill_start_date desc),' ') as cm_bill_salesman_cd     
     ,nvl(max(ph.pos_shop_cd),' ') as cm_ld_store_cd
     ,nvl(max(ph.salesman_cd),' ') as cm_ld_salesman_cd
     ,nvl(max(ph.online_inv_flg),' ') as online_inv_flg
     ,nvl(max(lmbs.bill_end_date),date '1900-01-01') as lm_bill_end_date
     ,' '  as gz_call_centre_flg
     ,date '2999-12-31'  as last_contact_date
    ,' ' as cm_cc_salesman_cd
    ,' ' as cm_cc_store_cd
    ,date '2999-12-31' as cm_cc_comm_date
    ,nvl(ph.inv_date,date '2999-12-31') as ld_inv_date
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004_T t
left outer join ${etlvar::ADWDB}.bill_servs cmbs
    on t.cm_rate_plan_cd = cmbs.bill_serv_cd
   and t.cm_subr_num = cmbs.subr_num
   and t.cm_cust_num = cmbs.cust_num
   and cmbs.bill_start_date <= &rpt_e_date
left outer join ${etlvar::ADWDB}.fes_usr_info cmuf
        on cmbs.salesman_cd = cmuf.usr_name
left outer join ${etlvar::ADWDB}.pos_inv_header ph   
    on t.ld_inv_num = ph.inv_num
left outer join ${etlvar::ADWDB}.bill_servs lmbs
    on t.lm_rate_plan_cd = lmbs.bill_serv_cd
   and t.lm_subr_num = lmbs.subr_num
   and t.lm_cust_num = lmbs.cust_num
   and &rpt_s_date - 1 between lmbs.bill_start_date and lmbs.bill_end_date
--left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005A_T cc
 --   on t.cm_subr_num = cc.subr_num
--   and t.cm_cust_num = cc.cust_num
--   and t.ph.inv_date between nvl(cc.last_contact_date,date '1900-01-01')
--                and  add_months(nvl(cc.last_contact_date,date '1900-01-01'),1) - 1
group by t.rpt_mth,t.case_id,nvl(ph.inv_date,date '2999-12-31');
commit;

prompt 'Step B_RETENT_UPG_COMM_005B_T : [ Mapping the additional info for call centre ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005B_T
(
     rpt_mth
    ,case_id
    ,cm_subr_num
    ,cm_cust_num
    ,lm_subr_num
    ,lm_cust_num
    ,ld_inv_num
    ,ld_start_date
    ,ld_inv_date
    ,cm_rate_plan_cd
    ,cm_bill_start_date
    ,cm_bill_store_cd
    ,cm_bill_salesman_cd
    ,cm_ld_store_cd
    ,cm_ld_salesman_cd
    ,online_inv_flg
    ,lm_bill_end_date
    ,gz_call_centre_flg
    ,last_contact_date
    ,cm_cc_salesman_cd
    ,cm_cc_store_cd
    ,cm_cc_comm_date
)
select  
     t.rpt_mth
    ,t.case_id
    ,t.cm_subr_num
    ,t.cm_cust_num
    ,t.lm_subr_num
    ,t.lm_cust_num
    ,t.ld_inv_num
    ,t.ld_start_date
    ,t.ld_inv_date
    ,t.cm_rate_plan_cd
    ,t.cm_bill_start_date
    ,t.cm_bill_store_cd
    ,t.cm_bill_salesman_cd
    ,t.cm_ld_store_cd
    ,t.cm_ld_salesman_cd
    ,t.online_inv_flg
    ,t.lm_bill_end_date
    ,case when max(cc.comm_user) is not null then 'Y' else 'N' end as gz_call_centre_flg
    ,nvl(max(cc.last_contact_date)  keep (dense_rank first order by comm_date desc),date '2999-12-31') as last_contact_date
    ,nvl(max(cc.comm_user) keep (dense_rank first order by comm_date desc),' ') as cm_cc_salesman_cd
    ,case when max(cc.comm_user) is not null then 'CALL_CENTRE' else ' ' end as cm_cc_store_cd
    ,nvl(max(cc.comm_date) keep (dense_rank first order by comm_date desc),date '2999-12-31') as cm_cc_comm_date
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005B01_T t
left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005A_T cc
        on t.cm_subr_num = cc.subr_num
        and t.cm_cust_num = cc.cust_num
        and round(greatest(t.ld_inv_date,t.cm_bill_start_date) - cc.comm_date) between 0 and 30 
group by 
             t.rpt_mth
    ,t.case_id
    ,t.cm_subr_num
    ,t.cm_cust_num
    ,t.lm_subr_num
    ,t.lm_cust_num
    ,t.ld_inv_num
    ,t.ld_start_date
    ,t.cm_rate_plan_cd
    ,t.cm_bill_start_date
    ,t.cm_bill_store_cd
    ,t.cm_bill_salesman_cd
    ,t.cm_ld_store_cd
    ,t.cm_ld_salesman_cd
    ,t.online_inv_flg
    ,t.lm_bill_end_date
    ,t.gz_call_centre_flg
    ,t.last_contact_date 
    ,t.ld_inv_date;
commit;



prompt 'Step B_RETENT_UPG_COMM_005C_T : [ Mapping the additional info for store cd and salesman cd ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005C_T
(
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
        ,ld_inv_date
        ,cm_fm_flg
        ,cm_fm_main_subr
        ,cm_fm_main_cust
        ,min_ukey_subr_sw_on_date
        ,flex_flg
        ,flex_inv
        ,flex_ld_start_date
        ,flex_end_date
        ,prnk
        ,cm_bill_cd_start_date
        ,lm_bill_cd_end_date
        ,cc_last_contact_date
        ,online_store_flg
        ,gz_call_centre_flg
        ,cm_store_cd
        ,cm_salesman_cd
        ,cm_store_json
        ,cm_salesman_json
        ,calc_rmk
        ,sales_type
        ,skip_flg
        ,skip_rmk 
        ,create_ts
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_cc_comm_date
        ,cm_fm_info
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
)select
         t.case_id
        ,t.sub_case_id_lst
        ,t.ukey
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_mthend_status
        ,t.lm_cust_num
        ,t.lm_subr_num
        ,t.lm_rate_plan_cd
        ,t.ld_start_date
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_exp_date
        ,lh.inv_date as ld_inv_date
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.min_ukey_subr_sw_on_date
        ,t.flex_flg
        ,t.flex_inv
        ,t.flex_ld_start_date
        ,t.flex_end_date
        ,t.prnk
        ,af.cm_bill_start_date
        ,af.lm_bill_end_date
        ,af.last_contact_date
        ,af.online_inv_flg as online_store_flg
        ,af.gz_call_centre_flg
        ,' ' as cm_store_cd
        ,' ' as cm_salesman_cd
        ,'{CM_BILL_STORE_CD:"'||af.cm_bill_store_cd
         ||'",CM_LD_STORE_CD:"'||af.cm_ld_store_cd
         ||'",FLEX_INV_STORE_CD:"'||fxp.pos_shop_cd
         ||'",ONLINE_STORE_CD:"'||ol.store_cd ||'"}'as cm_store_json
        ,'{CM_BILL_SALESMAN_CD:"'||af.cm_bill_salesman_cd
         ||'",CM_LD_SALESMAN_CD:"'||af.cm_ld_salesman_cd
         ||'",FLEX_INV_SALESMAN_CD:"'||fxp.salesman_cd
         ||'",ONLINE_SALESMAN_CD:"'||ol.salesman_cd||'"}' as cm_salesman_json
        ,' ' as calc_rmk
        ,' ' as sales_type
        ,' ' as skip_flg
        ,' ' as skip_rmk
        ,sysdate as create_ts
        ,af.cm_cc_salesman_cd
        ,af.cm_cc_store_cd
        ,af.cm_cc_comm_date
        ,t.cm_fm_info
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
from  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_004_T t
left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005B_T af
        on t.case_id = af.case_id
left outer join (
  Select  max(submission_date)
        ,subr_num
        ,cust_num
        ,new_contract_date
        ,max(salesman_cd) keep(dense_rank first order by submission_date desc) as salesman_cd
        ,max(store_cd) keep(dense_rank first order by submission_date desc) as store_cd
  from ${etlvar::ADWDB}.online_retent_portal_trans     
  group by subr_num,cust_num,new_contract_date
) ol
        on t.cm_subr_num = ol.subr_num
        and t.cm_cust_num = ol.cust_num
        and t.ld_start_date = ol.new_contract_date
left outer join ${etlvar::ADWDB}.pos_inv_header fxp
        on t.flex_inv = fxp.inv_num
        and t.flex_flg='Y'
left outer join ${etlvar::ADWDB}.pos_inv_header lh
        on t.ld_inv_num = lh.inv_num ;
commit;


prompt 'Step B_RETENT_UPG_COMM_005D_T : [ Mapping the fin RBD override table ]';

insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005D_T
(
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
        ,cm_bill_cd_start_date
        ,lm_bill_cd_end_date
        ,cc_last_contact_date
        ,online_store_flg
        ,gz_call_centre_flg
        ,cm_store_cd
        ,cm_salesman_cd
        ,cm_store_json
        ,cm_salesman_json
        ,calc_rmk
        ,sales_type
        ,skip_flg
        ,skip_rmk
        ,rbd_free_mth
        ,rbd_fixed_amt
        ,rbd_hs_subsidy
        ,rbd_overr_contract_mth
        ,rbd_overr_flg
        ,create_ts
        ,ld_inv_date
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_cc_comm_date
        ,cm_fm_info
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
)select
         case_id
        ,t.sub_case_id_lst
        ,t.ukey
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_mthend_status
        ,t.lm_cust_num
        ,t.lm_subr_num
        ,t.lm_rate_plan_cd
        ,t.ld_start_date
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_exp_date
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.min_ukey_subr_sw_on_date
        ,t.flex_flg
        ,t.flex_inv
        ,t.flex_ld_start_date
        ,t.flex_end_date
        ,t.prnk
        ,t.cm_bill_cd_start_date
        ,t.lm_bill_cd_end_date
        ,t.cc_last_contact_date
        ,t.online_store_flg
        ,t.gz_call_centre_flg
        ,t.cm_store_cd
        ,t.cm_salesman_cd
        ,t.cm_store_json
        ,t.cm_salesman_json
        ,t.calc_rmk
        ,t.sales_type
        ,' ' skip_flg --decode(nvl(rnp.ref_type,' ') ,'NO CHANGE','Y','NOT COUNT','Y',' ')  skip_flg
        ,' ' skip_rmk --decode(nvl(rnp.ref_type,' ') ,'NO CHANGE','RBD_NOCHANGE_PLAN;','NOT COUNT','RBD_NOT_COUNT;',' ')  skip_rmk
        ,0 as rbdd_free_mth ---nvl(b.free_mth,0) as rbd_free_mth
        ,nvl(c.comm_amt,0) as rbd_fixed_amt
        ,nvl(d.amt,0) as rbd_hs_subsidy
        ,nvl(e.overr_contract_mth ,0) as rbd_overr_contract_mth
        ,nvl(decode(b.free_mth,null,'','RBD_FREE_MTH;')
        ||decode(c.comm_amt,null,'','RBD_FIXED_AMT;')
        ||decode(d.amt,null,'','RBD_HS_SUBSIDY;')
        ||decode(e.overr_contract_mth,null,'','RBD_OVERRIDE_MKT_CD;')
        ||decode(rnp.ref_type,null,'','NO CHANGE','RBD_NO_CHANGE;','NOT COUNT','RBD_NOT_COUNT;')
        ,' ') as rbd_overr_flg
        ,t.create_ts
        ,t.ld_inv_date
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
        ,t.cm_cc_comm_date
        ,t.cm_fm_info
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
from  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005C_T t
left outer join ${etlvar::ADWDB}.RBD_NOCHANGE_PLAN rnp
        on (
            t.lm_rate_plan_cd = rnp.orig_bill_cd
        and t.cm_rate_plan_cd = rnp.new_bill_cd
        and upper(rnp.ref_type) ='NO CHANGE'
        and t.rpt_mth = rnp.trx_month)
        or (
            t.cm_rate_plan_cd = rnp.new_bill_cd
        and upper(rnp.ref_type) ='NOT COUNT'
        and t.rpt_mth = rnp.trx_month
        )
left outer join ${etlvar::ADWDB}.RBD_MKT_CD_FREE_MTH b
        on t.rpt_mth= b.Trx_Month
        and   t.ld_mkt_Cd = b.Mkt_Cd
left outer join ${etlvar::ADWDB}.RBD_BILL_CD_FIXED_AMT c
        on t.rpt_mth = c.Trx_Month
        and   t.cm_rate_plan_cd = c.Bill_Cd
left outer join ${etlvar::ADWDB}.RBD_HS_SUBSIDY d
        on t.rpt_mth = d.Trx_Month
        and   t.cm_rate_plan_cd = d.Bill_Cd
left outer join ${etlvar::ADWDB}.RBD_OVERRIDE_MKT_CD e
        on t.rpt_mth = e.Trx_Month
        and  t.ld_mkt_cd  = e.Mkt_cd ;
commit;

prompt 'Step B_RETENT_UPG_COMM_005E_T : [ Map additonal info part2 ]';
insert into  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005E_T
(
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
        ,cm_bill_cd_start_date
        ,lm_bill_cd_end_date
        ,cc_last_contact_date
        ,online_store_flg
        ,gz_call_centre_flg
        ,cm_store_cd
        ,cm_salesman_cd
        ,cm_store_json
        ,cm_salesman_json
        ,calc_rmk
        ,sales_type
        ,skip_flg
        ,skip_rmk
        ,rbd_free_mth
        ,rbd_fixed_amt
        ,rbd_hs_subsidy
        ,rbd_overr_contract_mth
        ,rbd_overr_flg
        ,ld_inv_date
        ,buy_out_from
        ,om_chg_plan_req_date
        ,om_chg_plan_next_mth
        ,create_ts
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_cc_comm_date
        ,cm_fm_info
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
)
select
         t.case_id
        ,t.sub_case_id_lst
        ,t.ukey
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_mthend_status
        ,t.lm_cust_num
        ,t.lm_subr_num
        ,t.lm_rate_plan_cd
        ,t.ld_start_date
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_exp_date
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.min_ukey_subr_sw_on_date
        ,t.flex_flg
        ,t.flex_inv
        ,t.flex_ld_start_date
        ,t.flex_end_date
        ,t.prnk
        ,t.cm_bill_cd_start_date
        ,t.lm_bill_cd_end_date
        ,t.cc_last_contact_date
        ,t.online_store_flg
        ,t.gz_call_centre_flg
        ,t.cm_store_cd
        ,t.cm_salesman_cd
        ,t.cm_store_json
        ,t.cm_salesman_json
        ,t.calc_rmk
        ,t.sales_type
        ,t.skip_flg
        ,t.skip_rmk
        ,t.rbd_free_mth
        ,t.rbd_fixed_amt
        ,t.rbd_hs_subsidy
        ,t.rbd_overr_contract_mth
        ,t.rbd_overr_flg
        ,t.ld_inv_date
        ,nvl(h.buy_out_num,' ') as buy_out_from
        ,nvl(o.create_date,date'2999-12-31') as om_chg_plan_req_date
        ,case when t.case_id like 'CLD_%' and omn.subr_num is not null then 'Y' else 'N' end om_chg_plan_next_mth
        ,t.create_ts
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
        ,t.cm_cc_comm_date
        ,t.cm_fm_info
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005D_T t
left outer join ${etlvar::ADWDB}.subr_info_hist h
        on t.cm_subr_num = h.subr_num
       and t.cm_cust_num = h.cust_num 
       and &rpt_e_date between h.start_date and h.end_date
left outer join (
    Select   omcp.cust_num
            ,omcp.subr_num
            ,max(omcp.eff_date) as eff_date
            ,omcp.new_plan_cd
            ,max(omcp.create_date) as create_date
    from ${etlvar::ADWDB}.om_complete_chg_plan omcp 
    where omcp.eff_date between &rpt_s_date and &rpt_e_date
    and omcp.new_plan_cd <> ' '
    group by omcp.cust_num
            ,omcp.subr_num
            ,omcp.new_plan_cd  
)o
    on  t.case_id like 'CPLAN%' 
    and t.cm_subr_num = o.subr_num
    and t.cm_cust_num = o.cust_num
    and t.cm_rate_plan_cd = new_plan_cd
left outer join (
     Select omcp2.cust_num
            ,omcp2.subr_num
            ,min(omcp2.eff_date)             
     from ${etlvar::ADWDB}.om_complete_chg_plan omcp2
     where omcp2.eff_date between add_months(&rpt_s_date,1) and add_months(&rpt_s_date,3)-1
     and omcp2.new_plan_cd <> ' '
     group by omcp2.cust_num
             ,omcp2.subr_num
     union all
     Select omp2.cust_num
            ,omp2.subr_num
            ,min(omp2.eff_date)             
     from ${etlvar::ADWDB}.om_pending_chg_plan omp2
     where omp2.eff_date between add_months(&rpt_s_date,1) and add_months(&rpt_s_date,3)-1
     and omp2.new_plan_cd <> ' '
     group by omp2.cust_num
             ,omp2.subr_num
)omn 
    on t.cm_subr_num = omn.subr_num
    and t.cm_cust_num = omn.cust_num ;
commit;


prompt 'Step B_RETENT_UPG_COMM_005_T : [ To result table ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005F_T
(
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
        ,cm_bill_cd_start_date
        ,lm_bill_cd_end_date
        ,cc_last_contact_date
        ,online_store_flg
        ,gz_call_centre_flg
        ,cm_store_cd
        ,cm_salesman_cd
        ,cm_store_json
        ,cm_salesman_json
        ,calc_rmk
        ,sales_type
        ,skip_flg
        ,skip_rmk
        ,rbd_free_mth
        ,rbd_fixed_amt
        ,rbd_hs_subsidy
        ,rbd_overr_contract_mth
        ,rbd_overr_flg
        ,ld_inv_date
        ,buy_out_from
        ,om_chg_plan_req_date
        ,om_chg_plan_next_mth
        ,cm_rebate
        ,create_ts
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_cc_comm_date
        ,cm_fm_info
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
)select
         t.case_id
        ,t.sub_case_id_lst
        ,t.ukey
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_mthend_status
        ,t.lm_cust_num
        ,t.lm_subr_num
        ,t.lm_rate_plan_cd
        ,t.ld_start_date
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_exp_date
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.min_ukey_subr_sw_on_date
        ,t.flex_flg
        ,t.flex_inv
        ,t.flex_ld_start_date
        ,t.flex_end_date
        ,t.prnk
        ,t.cm_bill_cd_start_date
        ,t.lm_bill_cd_end_date
        ,t.cc_last_contact_date
        ,t.online_store_flg
        ,t.gz_call_centre_flg
        ,t.cm_store_cd
        ,t.cm_salesman_cd
        ,t.cm_store_json
        ,t.cm_salesman_json
        ,t.calc_rmk
        ,t.sales_type
        ,t.skip_flg
        ,t.skip_rmk
        ,t.rbd_free_mth
        ,t.rbd_fixed_amt
        ,t.rbd_hs_subsidy
        ,t.rbd_overr_contract_mth
        ,t.rbd_overr_flg
        ,t.ld_inv_date
        ,t.buy_out_from
        ,t.om_chg_plan_req_date
        ,t.om_chg_plan_next_mth
        ,nvl(scb.ttl_cb_amt,0)as cm_rebate
        ,t.create_ts
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
        ,t.cm_cc_comm_date
        ,t.cm_fm_info
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005E_T t
left outer join (
        select tbl.case_id ,sum(tbl.cb_amt) as ttl_cb_amt
        from 
        (
        --------SCB simonly scb from mkt_cd ----------
        select t.case_id
                ,t.cm_subr_num
                ,t.cm_cust_num
                ,t.ld_mkt_cd
                ,t.ld_inv_date
                ,s.credit_back_cd
                ,max(s.credit_back_amt) cb_amt
        from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005E_T t
                    ,${etlvar::ADWDB}.pos_crbk_detail p
                    ,${etlvar::ADWDB}.srvcb s
        where
            t.cm_subr_num = p.subr_num 
         and t.cm_cust_num = p.cust_num
         and t.ld_mkt_cd = s.mkt_cd 
         and p.credit_cd = s.credit_back_cd
         and &rpt_e_date between s.start_date and s.end_date   
         and abs(t.ld_inv_date - p.crbk_date ) <=1
         and (p.inv_num = t.ld_inv_num or p.inv_num =' ') 
        group by 
        t.case_id
                ,t.cm_subr_num
                ,t.cm_cust_num
                ,t.ld_mkt_cd        
                  ,t.ld_inv_date
                  ,s.credit_back_cd
        --------FES HS BONUS credit back --------------
        union all
        select   
                t.case_id    
                ,t.cm_subr_num    
                ,t.cm_cust_num
                ,t.ld_mkt_cd                
                ,t.ld_inv_date
                ,bh.credit_back_cd
                ,sum(bh.credit_back_amt * d.qty)  as cb_amt
        from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005E_T t 
                ,${etlvar::ADWDB}.pos_inv_detail d       
                ,${etlvar::ADWDB}.fes_hs_bonus_hist bh
        where t.ld_inv_num = d.inv_num
              and t.ld_mkt_cd = bh.mkt_cd
              and d.pos_prod_cd = bh.prod_cd
              and &rpt_e_date between bh.start_date and bh.end_date  
        group by t.case_id        
                ,t.cm_cust_num
                ,t.cm_subr_num
                ,t.ld_inv_num
                ,t.ld_inv_date
                ,t.ld_mkt_cd
                ,bh.credit_back_cd
        ) tbl
        group by tbl.case_id
)scb on t.case_id = scb.case_id; 
commit;


prompt 'Step B_RETENT_UPG_COMM_005G_T : [ To result table ]';

insert into mig_adw.B_RETENT_UPG_COMM_005G_T
(
    case_id
    ,subr_num
    ,cust_num
    ,cm_fm_flg
    ,cm_fm_main_subr
    ,cm_fm_main_cust
    ,cc_line_type
    ,cc_overr_line_type
)    
select   t.case_id
        ,t.cm_subr_num
        ,t.cm_cust_num
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust        
        ,nvl(case 
              when t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
               and cm_port_type in ('PORT_MNP')
               then 'CC_NEW_LINE_MNP'
              when t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
               and cm_port_type in ('PORT_BIRDE','PORT_MVNO','PORT_HKBN','PORT_NSP')
               then 'CC_NEW_LINE_ALL'
              when lm_ld_inv_num <> ' ' 
               and lm_ld.ld_expired_date - cm_p.inv_date <= 180 then 'CC_RETENT_LINE'
              when lm_ld_inv_num <> ' '
               and lm_ld.ld_expired_date - cm_p.inv_date > 180 then 'CC_STIMULATE_LINE'
         end,'CC_RETENT_LINE') CC_LINE_TYPE                               
        ,nvl(case
              when t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
               and cm_port_type in ('PORT_MNP')
               then 'CC_NEW_LINE_MNP'
              when t.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date
               and cm_port_type in ('PORT_BIRDE','PORT_MVNO','PORT_HKBN','PORT_NSP')
               then 'CC_NEW_LINE'
              when lm_ld_inv_num <> ' '
               and lm_ld.ld_expired_date - cm_p.inv_date <= 180 then 'CC_RETENT_LINE'
              when lm_ld_inv_num <> ' '
               and lm_ld.ld_expired_date - cm_p.inv_date > 180 then 'CC_STIMULATE_LINE'
         end,'CC_RETENT_LINE') CC_OVERR_LINE_TYPE
from (select * from mig_adw.B_RETENT_UPG_COMM_005F_T where cm_fm_flg like 'FAMILY%')t
   left outer join  prd_adw.subr_ld_hist lm_ld
        on t.lm_ld_inv_num = lm_ld.inv_num
        and &rpt_s_date -1 between lm_ld.start_date and lm_ld.end_date
   left outer join  prd_adw.pos_inv_header cm_p
        on t.lm_ld_inv_num = lm_ld.inv_num
        and t.ld_inv_num = cm_p.inv_num;
commit;
-------------

----override the line type to retent if found one of family line is not NEW ----

update mig_adw.B_RETENT_UPG_COMM_005G_T t
set cc_overr_line_type ='CC_NEW_LINE_NOTALL'
where (cm_fm_main_subr,cm_fm_main_cust)
   in (
       Select t.cm_fm_main_subr
              ,t.cm_fm_main_cust
              --,sum(case when t2.cc_line_type in ('CC_RETENT_LINE','CC_STIMULATE_LINE') then 1 else 0 end ) chk_flg 
       from  mig_adw.B_RETENT_UPG_COMM_005G_T t   
            ,mig_adw.B_RETENT_UPG_COMM_005G_T t2
       where t.subr_num =t2.cm_fm_main_subr
       and t.cust_num = t2.cm_fm_main_cust   
       and t.cm_fm_flg='FAMILY_MAIN'  
       and t2.cm_fm_flg = 'FAMILY_SUBR'
       and t.cc_line_type like 'CC_NEW_LINE_ALL'
       group by t.cm_fm_main_subr,t.cm_fm_main_cust
       having sum(case when t2.cc_line_type in ('CC_RETENT_LINE','CC_STIMULATE_LINE') then 1 else 0 end ) >1 
      )
    and cc_line_type like 'CC_NEW_LINE_ALL';
commit;

-----override the cm_rebate , cm_hs_subsidy ---


update mig_adw.B_RETENT_UPG_COMM_005F_T t
set 
        (t.cm_rebate,t.rbd_hs_subsidy,calc_rmk) =
(
        select r.cm_rebate,r.rbd_hs_subsidy,t.calc_rmk||';OVERRIDE_FM_REBATE_SUBSIDY'
        from  mig_adw.B_RETENT_UPG_COMM_005F_T r
        where t.cm_fm_main_subr = r.cm_subr_num
          and t.cm_fm_main_cust = r.cm_cust_num
          and r.cm_fm_flg= 'FAMILY_MAIN'
)
where 
   (t.cm_fm_main_subr,t.cm_fm_main_cust)
   in ( select r2.cm_subr_num,r2.cm_cust_num 
        from  mig_adw.B_RETENT_UPG_COMM_005F_T r2 where r2.cm_fm_flg='FAMILY_MAIN')
and t.cm_fm_flg = 'FAMILY_SUB';

commit;

insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005_T
(
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
        ,cm_bill_cd_start_date
        ,lm_bill_cd_end_date
        ,cc_last_contact_date
        ,online_store_flg
        ,gz_call_centre_flg
        ,cm_store_cd
        ,cm_salesman_cd
        ,cm_store_json
        ,cm_salesman_json
        ,calc_rmk
        ,sales_type
        ,skip_flg
        ,skip_rmk
        ,rbd_free_mth
        ,rbd_fixed_amt
        ,rbd_hs_subsidy
        ,rbd_overr_contract_mth
        ,rbd_overr_flg
        ,ld_inv_date
        ,buy_out_from
        ,om_chg_plan_req_date
        ,om_chg_plan_next_mth
        ,cm_rebate
        ,create_ts
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_cc_comm_date
        ,cm_fm_info
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
        ,cm_cc_line_type
)select
         t.case_id
        ,t.sub_case_id_lst
        ,t.ukey
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_mthend_status
        ,t.lm_cust_num
        ,t.lm_subr_num
        ,t.lm_rate_plan_cd
        ,t.ld_start_date
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_exp_date
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.min_ukey_subr_sw_on_date
        ,t.flex_flg
        ,t.flex_inv
        ,t.flex_ld_start_date
        ,t.flex_end_date
        ,t.prnk
        ,t.cm_bill_cd_start_date
        ,t.lm_bill_cd_end_date
        ,t.cc_last_contact_date
        ,t.online_store_flg
        ,t.gz_call_centre_flg
        ,t.cm_store_cd
        ,t.cm_salesman_cd
        ,t.cm_store_json
        ,t.cm_salesman_json
        ,t.calc_rmk
        ,t.sales_type
        ,t.skip_flg
        ,t.skip_rmk
        ,t.rbd_free_mth
        ,t.rbd_fixed_amt
        ,t.rbd_hs_subsidy
        ,t.rbd_overr_contract_mth
        ,t.rbd_overr_flg
        ,t.ld_inv_date
        ,t.buy_out_from
        ,t.om_chg_plan_req_date
        ,t.om_chg_plan_next_mth
        ,t.cm_rebate
        ,t.create_ts
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
        ,t.cm_cc_comm_date
        ,t.cm_fm_info
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
        ,nvl(r.cc_overr_line_type,' ')
from mig_adw.B_RETENT_UPG_COMM_005F_T   t
left outer join mig_adw.B_RETENT_UPG_COMM_005G_T r
on t.case_id = r.case_id;
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


