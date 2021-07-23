/opt/etl/prd/etl/APP/ADW/B_RETENT_UPG_COMM_RPT/bin> cat b_retent_upg_comm_rpt0040.pl
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
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_006A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_006A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_006B_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_006C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_006C02_T');


set define on;
define rpt_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_s_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),1)-1;

DELETE FROM ${etlvar::MIGDB}.RETENT_UPG_COMM_H where rpt_mth = &rpt_mth ;
commit;

--------#####

--------##### Praparing change plan case  comparing for current month end and last month case ---------------------------------------------------------------
prompt 'Step B_RETENT_UPG_COMM_006A01_T : [ Get last prv retent comm base on 001_T profile image  ]';

insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_006A01_T
(
   case_id
  ,ukey
  ,rpt_mth
  ,cm_cust_num
  ,cm_subr_num
  ,prv_mthend_cust_num
  ,prv_mthend_subr_num
  ,prv_mthend
  ,prv_case_id
  ,prv_rpt_mth
  ,prv_retent_comm
  ,create_ts
 ,prv_ld_start_date
 ,prv_ld_inv_num
 ,prv_ld_cd   
 ,prv_ld_mkt_cd
 ,prv_ld_exp_date
 ,prv_ld_inv_date  
 ,prv_tariff  
 ,prv_paid_mth
 ,prv_tcv
 ,prv_ld_exp_date_cm
 ,prv_comm_mth
)
select 
   tm.case_id
  ,tm.ukey
  ,tm.rpt_mth
  ,tm.cm_cust_num
  ,tm.cm_subr_num
  ,tm.prv_mthend_cust_num
  ,tm.prv_mthend_subr_num
  ,tm.prv_mthend
  ,tm.prv_case_id
  ,tm.prv_rpt_mth
  ,tm.prv_retent_comm
  ,tm.create_ts
 ,tm.prv_ld_start_date
 ,tm.prv_ld_inv_num
 ,tm.prv_ld_cd
 ,tm.prv_ld_mkt_cd
 ,tm.prv_ld_exp_date
 ,tm.prv_ld_inv_date
 ,tm.prv_tariff
 ,tm.prv_paid_mth
 ,tm.prv_tcv
 ,nvl(sl.ld_expired_date,date '1900-01-01') as prv_ld_exp_date_cm
 ,tm.prv_comm_mth
from (
Select   t.case_id
        ,t.rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.ukey        
        ,max(p.cust_num) keep(dense_rank first order by h.rpt_mth)as prv_mthend_cust_num
        ,max(p.subr_num) keep(dense_rank first order by h.rpt_mth)as prv_mthend_subr_num
        ,max(ly.mthend) keep(dense_rank first order by h.rpt_mth) as prv_mthend
        ,max(h.case_id) keep(dense_rank first order by h.rpt_mth) as prv_case_id
        ,max(h.rpt_mth) keep(dense_rank first order by h.rpt_mth) as prv_rpt_mth
        ,max(h.retent_comm) keep(dense_rank first order by h.rpt_mth) as prv_retent_comm
        ,sysdate as create_ts
        ,max(h.ld_start_date) keep(dense_rank first order by h.rpt_mth)as prv_ld_start_date
        ,max(h.ld_inv_num) keep(dense_rank first order by h.rpt_mth)as prv_ld_inv_num
        ,max(h.ld_cd) keep(dense_rank first order by h.rpt_mth)as prv_ld_cd
        ,max(h.ld_mkt_cd) keep(dense_rank first order by h.rpt_mth)as prv_ld_mkt_cd
        ,max(h.ld_exp_date) keep(dense_rank first order by h.rpt_mth)as prv_ld_exp_date
        ,max(h.ld_inv_date) keep(dense_rank first order by h.rpt_mth)as prv_ld_inv_date
        ,max(h.tariff) keep(dense_rank first order by h.rpt_mth)as prv_tariff
        ,max(h.paid_mth) keep(dense_rank first order by h.rpt_mth)as prv_paid_mth
        ,max(h.tcv) keep(dense_rank first order by h.rpt_mth)as prv_tcv
        ,max(h.rpt_mth) keep(dense_rank first order by h.rpt_mth)as prv_comm_mth
  from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005_T t
      ,${etlvar::MIGDB}.B_RETENT_UPG_COMM_001_T p
      ,(select 
        add_months(&rpt_s_date,- rownum + 1)-1 mthend 
        from dual connect by rownum<=12) ly
      ,${etlvar::MIGDB}.RETENT_UPG_COMM_H h
 where t.ukey = p.ukey
   and ly.mthend between p.start_date and p.end_date
   and trunc(ly.mthend,'MM')  = h.rpt_mth
   and p.cust_num = h.cm_cust_num
   and p.subr_num = h.cm_subr_num
---- exclude the dummy case ----
   and h.case_id not like 'CDUMMY%'
---- exclude the skip case ----
   and h.skip_flg <> 'Y'
group by t.case_id
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.ukey
        ,t.rpt_mth
)tm
left outer join prd_adw.subr_ld_hist  sl
on tm.prv_ld_inv_num = sl.inv_num
and &rpt_e_date between sl.start_date and end_date;
commit;

--------##### Praparing change plan case  comparing for current month end and last month case ---------------------------------------------------------------
prompt 'Step B_RETENT_UPG_COMM_006A_T : [ Handle the skip flag,map tariff,decide the store cd and salesman cd  ]';
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_006A_T
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
        ,tariff
        ,ld_inv_date
        ,buy_out_from
        ,om_chg_plan_req_date
        ,om_chg_plan_next_mth
        ,cm_rebate
        ,fin_fm_sim_cnt
        ,fin_fm_sub_sim_plan_cd
        ,cm_fm_active_sim_cnt
        ,cm_rate_plan_rate
        ,cm_ld_contract_mth
        ,prv_case_id
        ,prv_retent_comm
        ,prv_ld_inv_num
        ,prv_ld_exp_date
        ,prv_tcv
        ,prv_ld_exp_date_cm
        ,prv_paid_mth
        ,prv_comm_mth
        ,create_ts
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
        ,cm_cc_line_type
        ,cm_fm_main_exist_flg
        ,cm_cc_comm_date
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
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
        ,nvl(case 
           when t.flex_flg = 'Y' then  
                json_value(cm_store_json,'\$.FLEX_INV_STORE_CD') 
           when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD') then
                json_value(cm_store_json,'\$.CM_LD_STORE_CD') 
           when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLAN') then
                json_value(cm_store_json,'\$.CM_BILL_STORE_CD') 
         else  ' '
        end,' ') cm_store_cd
        ,nvl(case 
           when t.flex_flg = 'Y' then  
                json_value(cm_salesman_json,'\$.FLEX_INV_SALESMAN_CD') 
           when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD') then
                json_value(cm_salesman_json,'\$.CM_LD_SALESMAN_CD') 
           when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLAN') then
                json_value(cm_salesman_json,'\$.CM_BILL_SALESMAN_CD') 
         else  ' ' 
        end,' ') cm_salesman_cd
        ,t.cm_store_json
        ,t.cm_salesman_json
        ,t.calc_rmk
        ,t.sales_type
        ,case 
        ----- Change plan within  3 month after sw on date
         when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD','CPLAN') 
          and abs(t.min_ukey_subr_sw_on_date - &rpt_s_date) < 90
         then 'Y'
        ----- Flex case but not first flex start month or flex end month
         when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD','CPLAN') 
          and flex_flg = 'Y' and &rpt_s_date not in (trunc(t.flex_ld_start_date,'MM'),trunc(t.flex_end_date,'MM'))
         then 'Y'
         when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD','CPLAN') 
          and instr(t.rbd_overr_flg,'RBD_NOT_COUNT;') > 0
         then 'Y'
         else ' '
         end skip_flg
        ,case 
        ----- Change plan within  3 month after sw on date
         when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD','CPLAN')
          and abs(t.min_ukey_subr_sw_on_date - &rpt_s_date) < 90
         then 'SKIP_CPLAN_WITHIN3MTH;'
        ----- Flex case but not first flex start month or flex end month
         when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD','CPLAN')
          and flex_flg = 'Y' and &rpt_s_date not in (trunc(t.flex_ld_start_date,'MM'),trunc(t.flex_end_date,'MM'))
         then 'SKIP_FLEX_IN_PD;'
         when substr(t.case_id,1,instr(t.case_id,'_') -1 ) in ('CPLANLD','CLD','CPLAN') 
          and instr(t.rbd_overr_flg,'RBD_NOT_COUNT;') > 0
         then 'SKIP_RBD_NOCOUNT;'
         else ' '
         end skip_rmk
        ,t.rbd_free_mth
        ,t.rbd_fixed_amt
        ,t.rbd_hs_subsidy
        ,t.rbd_overr_contract_mth
        ,t.rbd_overr_flg
        ,0 as tariff
        ,t.ld_inv_date
        ,t.buy_out_from
        ,t.om_chg_plan_req_date
        ,t.om_chg_plan_next_mth
        ,t.cm_rebate
        ,nvl(f.sim_count,0) as fin_fm_sim_cnt
        ,nvl(f.f_sub_plan_cd,' ') as fin_fm_sub_sim_plan_cd
        ,nvl(fm.cm_fm_active_sim_cnt,0)
        ,nvl(br.bill_rate,0) as cm_rate_plan_rate
        ,to_number(decode(t.ld_cd,' ',0,substr(t.ld_cd,4,2))) as cm_ld_contract_mth
        ,nvl(prv.prv_case_id,' ')
        ,nvl(prv.prv_retent_comm,0)
        ,nvl(prv.prv_ld_inv_num, ' ')
        ,nvl(prv.prv_ld_exp_date,date '1900-01-01')
        ,nvl(prv.prv_tcv,0)
        ,nvl(prv.prv_ld_exp_date_cm,date '1900-01-01')
        ,nvl(prv.prv_paid_mth,0)
        ,nvl(prv.prv_comm_mth,date '1900-01-01')
        ,sysdate as create_ts
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
        ,t.cm_cc_line_type
        ,nvl(ft.cm_fm_main_exist_flg,'N') cm_fm_main_exist_flg
        ,cm_cc_comm_date
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
from  ${etlvar::MIGDB}.B_RETENT_UPG_COMM_005_T t
left outer join ${etlvar::MIGDB}.B_RETENT_UPG_COMM_006A01_T prv
        on t.case_id = prv.case_id
left outer join ${etlvar::ADWDB}.bill_serv_ref br 
        on t.cm_rate_plan_cd = br.bill_serv_cd
        and &rpt_e_date between br.eff_start_date and br.eff_end_date
left outer join ${etlvar::MIGDB}.rbd_family_plan_ref f
        on t.cm_rate_plan_cd = f.f_main_plan_cd 
        and t.rpt_mth = f.trx_month
left outer join (
        select 
          mob_cust_num
         ,mob_subr_num
         ,count(distinct tablet_subr_num) cm_fm_active_sim_cnt
        from ${etlvar::ADWDB}.om_tagon_map tg
        where tg.tag_on_code = 'ADDON_VOICE'
        and &rpt_e_date between tg.start_date and tg.end_date
        group by mob_cust_num,mob_subr_num
)fm on t.cm_subr_num = fm.mob_subr_num
   and t.cm_cust_num = fm.mob_cust_num 
left outer join (
---- skip cc calculation 
        Select   ft1.case_id
                ,case when sum(decode(ft2.cm_fm_flg,'FAMILY_MAIN',1 ,0)) > 0 then 'Y' else 'N' end  cm_fm_main_exist_flg 
        from mig_adw.B_RETENT_UPG_COMM_005_T ft1
        left outer join mig_adw.B_RETENT_UPG_COMM_005_T ft2
        on ft1.cm_fm_main_subr = ft2.cm_subr_num
           and ft2.cm_fm_main_cust = ft2.cm_cust_num 
           and ft1.cm_fm_flg = 'FAMILY_SUB'
           and ft2.cm_fm_flg = 'FAMILY_MAIN'
        where ft1.cm_fm_flg like 'FAMILY_%'
        group by ft1.case_id 
)ft  on t.case_id = ft.case_id
;

---------skip the cc comm caluclation if ever apply a dummy cc commission but reserver the normal commission calucaltion ------
update mig_adw.B_RETENT_UPG_COMM_006A_T tt
set      tt.cm_fm_main_exist_flg ='N'
        ,tt.calc_rmk =tt.calc_rmk ||';paid cc dummy comm override cc_comm to 0 ;'
where tt.case_id in 
(
        Select distinct t.case_id
          from mig_adw.B_RETENT_UPG_COMM_006A_T t              
              ,mig_adw.RETENT_UPG_COMM_H cdummy
          where t.cm_subr_num = cdummy.cm_subr_num
            and t.cm_cust_num = cdummy.cm_cust_num
            and t.cm_fm_flg like 'FAMILY%'
            and t.gz_call_centre_flg ='Y'
            and t.rpt_mth - cdummy.rpt_mth <= 365 
            and t.prv_comm_mth < t.rpt_mth
            and cdummy.case_id like 'CDUMMY%'
            and cdummy.rpt_mth < t.rpt_mth
);
commit;

prompt 'Step B_RETENT_UPG_COMM_006B_T : [ handling the calculation value ]';
declare 
    cursor cur_t is 
        select * from mig_adw.B_RETENT_UPG_COMM_006A_T ;
    rs_sr  mig_adw.B_RETENT_UPG_COMM_006A_T%ROWTYPE;
    rs_ta  mig_adw.B_RETENT_UPG_COMM_006B_T%ROWTYPE;
    commit_cnt integer;     
begin
    commit_cnt:=0;
    open cur_t;
    loop
        commit_cnt:= commit_cnt + 1;
        fetch cur_t into rs_sr;
        exit when cur_t%notfound;  
        ----- Non calculate column
            rs_ta.case_id := rs_sr.case_id;
            rs_ta.sub_case_id_lst := rs_sr.sub_case_id_lst;
            rs_ta.ukey := rs_sr.ukey;
            rs_ta.rpt_mth := rs_sr.rpt_mth;
            rs_ta.cm_cust_num := rs_sr.cm_cust_num;
            rs_ta.cm_subr_num := rs_sr.cm_subr_num;
            rs_ta.cm_rate_plan_cd := rs_sr.cm_rate_plan_cd;
            rs_ta.cm_mthend_status := rs_sr.cm_mthend_status;
            rs_ta.lm_cust_num := rs_sr.lm_cust_num;
            rs_ta.lm_subr_num := rs_sr.lm_subr_num;
            rs_ta.lm_rate_plan_cd := rs_sr.lm_rate_plan_cd;
            rs_ta.ld_start_date := rs_sr.ld_start_date;
            rs_ta.ld_inv_num := rs_sr.ld_inv_num;
            rs_ta.ld_cd := rs_sr.ld_cd;
            rs_ta.ld_mkt_cd := rs_sr.ld_mkt_cd;
            rs_ta.ld_exp_date := rs_sr.ld_exp_date;
            rs_ta.ld_inv_date := rs_sr.ld_inv_date;
            rs_ta.cm_fm_flg := rs_sr.cm_fm_flg;
            rs_ta.cm_fm_main_subr := rs_sr.cm_fm_main_subr;
            rs_ta.cm_fm_main_cust := rs_sr.cm_fm_main_cust;
            rs_ta.min_ukey_subr_sw_on_date := rs_sr.min_ukey_subr_sw_on_date;
            rs_ta.flex_flg := rs_sr.flex_flg;
            rs_ta.flex_inv := rs_sr.flex_inv;
            rs_ta.flex_ld_start_date := rs_sr.flex_ld_start_date;
            rs_ta.flex_end_date := rs_sr.flex_end_date;
            rs_ta.prnk := rs_sr.prnk;
            rs_ta.cm_bill_cd_start_date := rs_sr.cm_bill_cd_start_date;
            rs_ta.lm_bill_cd_end_date := rs_sr.lm_bill_cd_end_date;
            rs_ta.cc_last_contact_date := rs_sr.cc_last_contact_date;
            rs_ta.online_store_flg := rs_sr.online_store_flg;
            rs_ta.gz_call_centre_flg := rs_sr.gz_call_centre_flg;
            rs_ta.cm_store_cd := rs_sr.cm_store_cd;
            rs_ta.cm_salesman_cd := rs_sr.cm_salesman_cd;
            rs_ta.calc_rmk := rs_sr.calc_rmk;
            rs_ta.sales_type := rs_sr.sales_type;
            rs_ta.cm_store_json := rs_sr.cm_store_json;
            rs_ta.cm_salesman_json := rs_sr.cm_salesman_json;
            rs_ta.skip_flg := rs_sr.skip_flg;
            rs_ta.skip_rmk := rs_sr.skip_rmk;
            rs_ta.rbd_free_mth := rs_sr.rbd_free_mth;
            rs_ta.rbd_fixed_amt := rs_sr.rbd_fixed_amt;
            rs_ta.rbd_hs_subsidy := rs_sr.rbd_hs_subsidy;
            rs_ta.rbd_overr_contract_mth := rs_sr.rbd_overr_contract_mth;
            rs_ta.rbd_overr_flg := rs_sr.rbd_overr_flg;
            rs_ta.buy_out_from := rs_sr.buy_out_from;
            rs_ta.om_chg_plan_next_mth := rs_sr.om_chg_plan_next_mth;
            rs_ta.om_chg_plan_req_date := rs_sr.om_chg_plan_req_date;
            rs_ta.cm_rebate := rs_sr.cm_rebate;
            rs_ta.fin_fm_sim_cnt := rs_sr.fin_fm_sim_cnt;
            rs_ta.fin_fm_sub_sim_plan_cd := rs_sr.fin_fm_sub_sim_plan_cd;
            rs_ta.cm_fm_active_sim_cnt := rs_sr.cm_fm_active_sim_cnt; 
            rs_ta.cm_rate_plan_rate := rs_sr.cm_rate_plan_rate;
            rs_ta.cm_ld_contract_mth := rs_sr.cm_ld_contract_mth;
            rs_ta.prv_case_id := rs_sr.prv_case_id;
            rs_ta.prv_retent_comm := rs_sr.prv_retent_comm;
            rs_ta.create_ts := rs_sr.create_ts;
        ------ New column -------------------------
            rs_ta.lm_ld_inv_num := rs_sr.lm_ld_inv_num;
            rs_ta.lm_ld_cd := rs_sr.lm_ld_cd;
            rs_ta.lm_ld_exp_date := rs_sr.lm_ld_exp_date;
            rs_ta.cm_port_type := rs_sr.cm_port_type;
            rs_ta.cm_cc_line_type := rs_sr.cm_cc_line_type;                                    
            rs_ta.prv_ld_inv_num := rs_sr.prv_ld_inv_num;
            rs_ta.prv_ld_exp_date := rs_sr.prv_ld_exp_date;
            rs_ta.prv_ld_exp_date_cm := rs_sr.prv_ld_exp_date_cm;
            rs_ta.prv_tcv := rs_sr.prv_tcv;
            rs_ta.prv_paid_mth := rs_sr.prv_paid_mth;
            rs_ta.prv_comm_mth := rs_sr.prv_comm_mth;
            rs_ta.cc_calc_remark := ' ';
            rs_ta.cm_fm_main_exist_flg := rs_sr.cm_fm_main_exist_flg;
            rs_ta.cm_cc_comm_date := rs_sr.cm_cc_comm_date;
            rs_ta.cm_cc_salesman_cd := rs_sr.cm_cc_salesman_cd;
            rs_ta.cm_cc_store_cd := rs_sr.cm_cc_store_cd;
        ------ calculate column--------------------
            rs_ta.tariff := rs_sr.cm_rate_plan_rate / case when rs_ta.cm_fm_flg like 'FAMILY%' then greatest(rs_ta.fin_fm_sim_cnt,1) else 1 end;
            rs_ta.remain_contract_mth := greatest(nvl(round((rs_ta.ld_exp_date - greatest(rs_ta.cm_bill_cd_start_date,nvl(rs_ta.ld_start_date,date '1900-01-01')))*12/365,1),0),1);
            rs_ta.paid_mth := greatest(case when instr(rs_ta.rbd_overr_flg,'RBD_OVERRIDE_MKT_CD;') > 0 
                              then rs_ta.rbd_overr_contract_mth
                              else nvl(rs_ta.remain_contract_mth - rs_sr.rbd_free_mth,0)
                              end,1);
            rs_ta.cm_hs_subsidy := case when rs_ta.ld_cd <> ' ' and rs_ta.cm_ld_contract_mth < to_number(substr(rs_ta.ld_cd,4,2))
                                        then nvl(rs_ta.rbd_hs_subsidy 
                                             / to_number(substr(rs_ta.ld_cd,4,2))
                                             * rs_ta.remain_contract_mth,0)
                                    else nvl(rs_ta.rbd_hs_subsidy,0) end;
            rs_ta.std_tcv := greatest(0,(rs_ta.tariff  - rs_ta.cm_rebate ) * rs_ta.paid_mth 
                                - (rs_ta.cm_hs_subsidy/case when rs_ta.cm_fm_flg like 'FAMILY%' then greatest(rs_ta.fin_fm_sim_cnt,1) else 1 end   ));
            rs_ta.tcv := greatest(0,(rs_ta.tariff  - rs_ta.cm_rebate ) * rs_ta.paid_mth 
                                - (rs_ta.cm_hs_subsidy/case when rs_ta.cm_fm_flg like 'FAMILY%' then greatest(rs_ta.fin_fm_sim_cnt,1) else 1 end   ));
            ----- total contract value - prv ttl contract remain value
            rs_ta.prv_tcv_remain := rs_ta.prv_tcv
                    * (greatest(rs_ta.prv_paid_mth - (rs_ta.rpt_mth - rs_ta.prv_comm_mth  ),0)/case when rs_ta.prv_paid_mth = 0 then 1 else rs_ta.prv_paid_mth end );                     
            rs_ta.arpu := case when rs_ta.remain_contract_mth = 0 then 0 else (rs_ta.tcv - rs_ta.prv_tcv_remain) /rs_ta.remain_contract_mth end;                   
            ----- Calculate the cc(call centre) commission -----
            ---Retention Line---
            ------ CC_COMM on apply to family plan                                     
            if rs_sr.cm_fm_main_exist_flg ='N' then
            ----skip gz call centre commission if main sim no in current month 
                rs_ta.cc_comm := 0;
            elsif rs_sr.gz_call_centre_flg = 'Y' and rs_sr.cm_cc_line_type = 'CC_RETENT_LINE' then               
                rs_ta.cc_comm := rs_ta.tcv / 2160 * 0.07 ;
            elsif rs_sr.gz_call_centre_flg= 'Y' and rs_sr.cm_cc_line_type = 'CC_STIMULATE_LINE' then
                ----compare last months between commision month and now . check how many paid month ratio left for last time,---- 
                rs_ta.cc_comm := rs_ta.tcv - rs_ta.prv_tcv_remain;
            elsif rs_sr.gz_call_centre_flg= 'Y' and rs_sr.cm_cc_line_type = 'CC_NEW_LINE_NOTALL' then                
                rs_ta.cc_comm := rs_ta.tcv / 2160 * 0.29 ;
            elsif rs_sr.gz_call_centre_flg= 'Y' and rs_sr.cm_cc_line_type = 'CC_NEW_LINE_ALL' then
                rs_ta.cc_comm := rs_ta.tcv / 2160 * 1 ;
            elsif rs_sr.gz_call_centre_flg= 'Y' and rs_sr.cm_cc_line_type = 'CC_NEW_LINE_MNP' then
                rs_ta.cc_comm := rs_ta.tcv / 2160 * 1 ;    
            else rs_ta.cc_comm := 0;
            end if;            
            rs_ta.comm := round(greatest(case 
                               when instr(rs_ta.rbd_overr_flg,'RBD_FIXED_AMT;') > 0
                                then rs_ta.rbd_fixed_amt  
                               when rs_ta.cm_ld_contract_mth > 24 
                                then rs_ta.arpu * 24 * 0.008   * rs_ta.remain_contract_mth / rs_ta.cm_ld_contract_mth
                               when rs_ta.cm_ld_contract_mth <= 24 
                                then rs_ta.arpu * rs_ta.remain_contract_mth *   0.008 
                          end,0),0);
            rs_ta.prv_retent_comm := 0;/*nvl(case 
                                        ---- Override case ---
                                        when instr(rs_ta.rbd_overr_flg,'RBD_OVERRIDE_MKT_CD;') > 0 
                                          then 0
                                        ---- No prv case ---
                                        when rs_sr.prv_ld_inv_num = ' '
                                          then 0
                                        ---- LD doesn't change only upgrade the plan ---
                                        when rs_sr.prv_ld_inv_num = rs_sr.ld_inv_num
                                          then (greatest(rs_sr.cm_bill_cd_start_date , rs_sr.ld_start_date) - rs_sr.prv_ld_exp_date_cm + 1) * 12 / 365
                                        when  rs_sr.prv_ld_inv_num <> ' ' and rs_sr.ld_start_date >= rs_sr.prv_ld_exp_date 
                                          then 0
                                        when  rs_sr.prv_ld_inv_num <> ' ' and rs_sr.ld_start_date < rs_sr.prv_ld_exp_date 
                                          then (rs_sr.prv_ld_exp_date - rs_sr.prv_ld_exp_date_cm + 1)   * 12 / 365
                                        else 0
                                     end * rs_ta.prv_tcv,0);*/
            rs_ta.prv_case_id := case when instr(rs_ta.rbd_overr_flg,'RBD_OVERRIDE_MKT_CD;') > 0 then ' ' else rs_ta.prv_case_id end ;
            rs_ta.retent_comm := greatest(nvl(rs_ta.comm,0) - nvl(rs_ta.prv_retent_comm,0),0);
            rs_ta.comm_type :=  case when rs_sr.min_ukey_subr_sw_on_date between &rpt_s_date and &rpt_e_date then 'NEWACT'
                                           else 'UPGRENT'
                                        end;
            rs_ta.comm_channel:=  case when rs_sr.cm_salesman_cd like 'FA%' or  rs_sr.cm_salesman_cd like 'WA%' then 'DSCM'
                                        when rs_sr.cm_salesman_cd like 'SA%' or rs_sr.cm_salesman_cd like 'S%' then 'RBD'
                                        else 'OTHER'
                                        end;
        ------- Calculate column finish
        ------ insert into result table 
            insert into mig_adw.B_RETENT_UPG_COMM_006B_T
            (      case_id
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
                  ,calc_rmk
                  ,sales_type
                  ,create_ts
                  ,cm_store_json
                  ,cm_salesman_json
                  ,skip_flg
                  ,skip_rmk
                  ,rbd_free_mth
                  ,rbd_fixed_amt
                  ,rbd_hs_subsidy
                  ,rbd_overr_contract_mth
                  ,rbd_overr_flg
                  ,buy_out_from
                  ,om_chg_plan_next_mth
                  ,om_chg_plan_req_date
                  ,remain_contract_mth
                  ,paid_mth
                  ,tcv
                  ,comm
                  ,retent_comm
                  ,cm_rebate
                  ,fin_fm_sim_cnt
                  ,fin_fm_sub_sim_plan_cd
                  ,cm_fm_active_sim_cnt
                  ,cm_rate_plan_rate
                  ,arpu
                  ,cm_ld_contract_mth
                  ,prv_case_id
                  ,prv_retent_comm
                  ,tariff
                  ,cm_hs_subsidy
                  ,std_tcv
                  ,prv_ld_inv_num
                  ,prv_ld_exp_date
                  ,prv_ld_exp_date_cm
                  ,prv_tcv
                  ,comm_type
                ,comm_channel
                ,lm_ld_inv_num
                  ,lm_ld_cd
                  ,lm_ld_exp_date
                  ,cm_port_type
                  ,cm_cc_line_type
                  ,cc_comm
                  ,cc_calc_remark
                  ,prv_paid_mth
                  ,prv_comm_mth
                  ,prv_tcv_remain
                 ,cm_fm_main_exist_flg
                ,cm_cc_comm_date
                ,cm_cc_salesman_cd
                ,cm_cc_store_cd
            ) values(
                   rs_ta.case_id
                  ,rs_ta.sub_case_id_lst
                  ,rs_ta.ukey
                  ,rs_ta.rpt_mth
                  ,rs_ta.cm_cust_num
                  ,rs_ta.cm_subr_num
                  ,rs_ta.cm_rate_plan_cd
                  ,rs_ta.cm_mthend_status
                  ,rs_ta.lm_cust_num
                  ,rs_ta.lm_subr_num
                  ,rs_ta.lm_rate_plan_cd
                  ,rs_ta.ld_start_date
                  ,rs_ta.ld_inv_num
                  ,rs_ta.ld_cd
                  ,rs_ta.ld_mkt_cd
                  ,rs_ta.ld_exp_date
                  ,rs_ta.ld_inv_date
                  ,rs_ta.cm_fm_flg
                  ,rs_ta.cm_fm_main_subr
                  ,rs_ta.cm_fm_main_cust
                  ,rs_ta.min_ukey_subr_sw_on_date
                  ,rs_ta.flex_flg
                  ,rs_ta.flex_inv
                  ,rs_ta.flex_ld_start_date
                  ,rs_ta.flex_end_date
                  ,rs_ta.prnk
                  ,rs_ta.cm_bill_cd_start_date
                  ,rs_ta.lm_bill_cd_end_date
                  ,rs_ta.cc_last_contact_date
                  ,rs_ta.online_store_flg
                  ,rs_ta.gz_call_centre_flg
                  ,rs_ta.cm_store_cd
                  ,rs_ta.cm_salesman_cd
                  ,rs_ta.calc_rmk
                  ,rs_ta.sales_type
                  ,rs_ta.create_ts
                  ,rs_ta.cm_store_json
                  ,rs_ta.cm_salesman_json
                  ,rs_ta.skip_flg
                  ,rs_ta.skip_rmk
                  ,rs_ta.rbd_free_mth
                  ,rs_ta.rbd_fixed_amt
                  ,rs_ta.rbd_hs_subsidy
                  ,rs_ta.rbd_overr_contract_mth
                  ,rs_ta.rbd_overr_flg
                  ,rs_ta.buy_out_from
                  ,rs_ta.om_chg_plan_next_mth
                  ,rs_ta.om_chg_plan_req_date
                  ,rs_ta.remain_contract_mth
                  ,rs_ta.paid_mth
                  ,rs_ta.tcv
                  ,rs_ta.comm
                  ,rs_ta.retent_comm
                  ,rs_ta.cm_rebate
                  ,rs_ta.fin_fm_sim_cnt
                  ,rs_ta.fin_fm_sub_sim_plan_cd
                  ,rs_ta.cm_fm_active_sim_cnt
                  ,rs_ta.cm_rate_plan_rate
                  ,rs_ta.arpu
                  ,rs_ta.cm_ld_contract_mth
                  ,rs_ta.prv_case_id
                  ,rs_ta.prv_retent_comm
                  ,rs_ta.tariff
                  ,rs_ta.cm_hs_subsidy
                  ,rs_ta.std_tcv
                  ,rs_ta.prv_ld_inv_num
                  ,rs_ta.prv_ld_exp_date
                  ,rs_ta.prv_ld_exp_date_cm
                  ,rs_ta.prv_tcv
                  ,rs_ta.comm_type
                  ,rs_ta.comm_channel
                  ,rs_ta.lm_ld_inv_num
                  ,rs_ta.lm_ld_cd
                  ,rs_ta.lm_ld_exp_date
                  ,rs_ta.cm_port_type
                  ,rs_ta.cm_cc_line_type
                  ,rs_ta.cc_comm
                  ,rs_ta.cc_calc_remark
                  ,rs_ta.prv_paid_mth
                  ,rs_ta.prv_comm_mth
                  ,rs_ta.prv_tcv_remain
                  ,rs_ta.cm_fm_main_exist_flg
        ,rs_ta.cm_cc_comm_date
        ,rs_ta.cm_cc_salesman_cd
        ,rs_ta.cm_cc_store_cd
            );
            if mod(commit_cnt,5000) = 0 then
                commit;
            end if;            
    end loop;
    commit;
    close cur_t;
end;
/
commit;
---------------------------------------------
----patch the  pos invoice dummy code case ----
insert into ${etlvar::MIGDB}.B_RETENT_UPG_COMM_006C01_T(
         case_id
        ,cm_cust_num
        ,cm_subr_num
        ,cm_rate_plan_cd
        ,cm_fm_flg
        ,cm_fm_main_subr
        ,cm_fm_main_cust
        ,prnk
        ,cc_last_contact_date
        ,gz_call_centre_flg
        ,cm_cc_comm_date
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_fm_info
        ,cm_port_type
        ,cm_cc_pos_inv_num
        ,cm_cc_pos_inv_date
        ,cm_cc_ld_cd
        ,cm_cc_rate_plan_rate
        ,cc_comm
        ,cc_dummy_flg
)    
select 
       'CDUMMY_'||p.cust_num||'_'||p.subr_num||'_'||to_char(&rpt_mth,'yymmdd')||'_'||p.inv_num as case_id
       ,p.cust_num
       ,p.subr_num
       ,p.rate_plan_cd
       ,'Y' as cm_fm_flg
       ,p.subr_num as cm_fm_main_subr
       ,p.cust_num as cm_fm_main_cust
       ,1 as prnk
       ,p.inv_date as cc_last_contact_date
       ,'Y' as gz_call_centre_flg
       ,p.inv_date as cm_cc_comm_date
       ,p.salesman_cd as cm_cc_salesman_cd
       ,'CALL_CENTRE' as cm_cc_store_cd
       ,'FM_DUMMY_CASE' as cm_fm_info
       ,' ' as cm_port_type
       ,p.inv_num as cm_cc_pos_inv_num
       ,p.inv_date as cm_cc_pos_inv_date
       ,' ' as cm_cc_ld_cd
       ,r.bill_rate as cm_cc_rate_plan_rate
       ,0 as cc_comm
       ,'Y' as cc_dummy_flg
from prd_adw.pos_inv_header p 
    ,prd_adw.bill_serv_ref r 
where p.mkt_cd in ('MVR70','MVR73','MVR76')
and p.inv_date between &rpt_s_date and &rpt_e_date
and (p.cust_num,p.subr_num ) not in (select t.cm_cust_num,t.cm_subr_num from mig_adw.B_RETENT_UPG_COMM_006B_T t)
and p.rate_plan_cd = r.bill_serv_cd
and p.inv_date between r.eff_start_date and r.eff_end_date
and p.inv_num not in (select inv_num from prd_adw.pos_return_header where trx_date between &rpt_s_date and &rpt_e_date);
commit;

insert into mig_adw.B_RETENT_UPG_COMM_006C02_T
 (
         case_id
        ,cm_cust_num
        ,cm_subr_num
        ,cm_rate_plan_cd
        ,cm_fm_flg
        ,cm_fm_main_subr
        ,cm_fm_main_cust
        ,prnk
        ,cc_last_contact_date
        ,gz_call_centre_flg
        ,cm_cc_comm_date
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
        ,cm_fm_info
        ,cm_port_type
        ,cm_cc_pos_inv_num
        ,cm_cc_pos_inv_date
        ,cm_cc_ld_cd
        ,cm_cc_rate_plan_rate
        ,cc_comm
        ,cc_dummy_flg
 ) select         
         t.case_id
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.prnk
        ,t.cc_last_contact_date
        ,t.gz_call_centre_flg
        ,t.cm_cc_comm_date
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
        ,t.cm_fm_info
        ,t.cm_port_type
        ,t.cm_cc_pos_inv_num
        ,t.cm_cc_pos_inv_date
        ,nvl(max(omp.ld_cd) keep (dense_rank first order by create_date),' ') as cm_cc_ld_cd
        ,t.cm_cc_rate_plan_rate
        ,t.cc_comm
        ,t.cc_dummy_flg
 from  mig_adw.B_RETENT_UPG_COMM_006C01_T t
 left outer join
 ( select cust_num
                ,subr_num
                ,eff_date
                ,create_date
                ,create_by
                ,handle_by
                ,pending_form
                ,new_plan_cd
                ,mkt_cd
                ,ld_cd 
        from prd_adw.om_pending_chg_plan
        where create_date between &rpt_s_date and &rpt_e_date          
        union all
        select cust_num
                ,subr_num
                ,eff_date
                ,create_date
                ,create_by
                ,handle_by
                ,pending_form
                ,new_plan_cd
                ,mkt_cd
                ,ld_cd 
        from prd_adw.om_complete_chg_plan
        where create_date between &rpt_s_date and &rpt_e_date         
 ) omp
 on t.cm_subr_num = omp.subr_num
 and t.cm_cust_num = omp.cust_num
 and t.cm_cc_pos_inv_date = omp.create_date
 and omp.ld_cd <> ' '
group by
         t.case_id
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,t.prnk
        ,t.cc_last_contact_date
        ,t.gz_call_centre_flg
        ,t.cm_cc_comm_date
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
        ,t.cm_fm_info
        ,t.cm_port_type
        ,t.cm_cc_pos_inv_num
        ,t.cm_cc_pos_inv_date
        ,t.cm_cc_rate_plan_rate
        ,t.cc_comm
        ,t.cc_dummy_flg ;
commit;

update mig_adw.B_RETENT_UPG_COMM_006C02_T t
set      tcv =  t.cm_cc_rate_plan_rate * to_number(nvl(substr(t.cm_cc_ld_cd,4,2),12))
        ,cc_comm= t.cm_cc_rate_plan_rate * to_number(nvl(substr(t.cm_cc_ld_cd,4,2),12)) /2160 * 0.07;
commit;


---------------------------------------------
prompt '[Insert into final table ]'
insert into ${etlvar::MIGDB}.RETENT_UPG_COMM_H (
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
        ,calc_rmk
        ,sales_type
        ,create_ts
        ,cm_store_json
        ,cm_salesman_json
        ,skip_flg
        ,skip_rmk
        ,rbd_free_mth
        ,rbd_fixed_amt
        ,rbd_hs_subsidy
        ,rbd_overr_contract_mth
        ,rbd_overr_flg
        ,buy_out_from
        ,om_chg_plan_next_mth
        ,om_chg_plan_req_date
        ,remain_contract_mth
        ,paid_mth
        ,tcv
        ,comm
        ,retent_comm
        ,cm_rebate
        ,fin_fm_sim_cnt
        ,fin_fm_sub_sim_plan_cd
        ,cm_fm_active_sim_cnt
        ,cm_rate_plan_rate
        ,arpu
        ,cm_ld_contract_mth
        ,prv_case_id
        ,prv_retent_comm
        ,tariff
        ,cm_hs_subsidy
        ,prv_ld_inv_num
        ,prv_ld_exp_date
        ,prv_ld_exp_date_cm
        ,comm_type
        ,comm_channel
        ,prv_tcv
        ,std_tcv
        ,lm_ld_inv_num
        ,lm_ld_cd
        ,lm_ld_exp_date
        ,cm_port_type
        ,cm_cc_line_type
        ,cc_comm
        ,cc_calc_remark
        ,prv_paid_mth
        ,prv_comm_mth
        ,prv_tcv_remain
        ,cm_fm_main_exist_flg
        ,cm_cc_comm_date
        ,cm_cc_salesman_cd
        ,cm_cc_store_cd
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
        ,t.ld_inv_date
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
        ,t.calc_rmk
        ,i.pos_sal_cat_cd as sales_type
        ,t.create_ts
        ,t.cm_store_json
        ,t.cm_salesman_json
        ,t.skip_flg
        ,t.skip_rmk
        ,t.rbd_free_mth
        ,t.rbd_fixed_amt
        ,t.rbd_hs_subsidy
        ,t.rbd_overr_contract_mth
        ,t.rbd_overr_flg
        ,t.buy_out_from
        ,t.om_chg_plan_next_mth
        ,t.om_chg_plan_req_date
        ,t.remain_contract_mth
        ,t.paid_mth
        ,t.tcv
        ,t.comm
        ,t.retent_comm
        ,t.cm_rebate
        ,t.fin_fm_sim_cnt
        ,t.fin_fm_sub_sim_plan_cd
        ,t.cm_fm_active_sim_cnt
        ,t.cm_rate_plan_rate
        ,t.arpu
        ,t.cm_ld_contract_mth
        ,t.prv_case_id
        ,t.prv_retent_comm
        ,t.tariff
        ,t.cm_hs_subsidy
        ,t.prv_ld_inv_num
        ,t.prv_ld_exp_date
        ,t.prv_ld_exp_date_cm
        ,t.comm_type
        ,t.comm_channel
        ,t.prv_tcv
        ,t.std_tcv
        ,t.lm_ld_inv_num
        ,t.lm_ld_cd
        ,t.lm_ld_exp_date
        ,t.cm_port_type
        ,t.cm_cc_line_type
        ,t.cc_comm
        ,t.cc_calc_remark
        ,t.prv_paid_mth
        ,t.prv_comm_mth
        ,t.prv_tcv_remain
        ,t.cm_fm_main_exist_flg
        ,t.cm_cc_comm_date
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd
from ${etlvar::MIGDB}.B_RETENT_UPG_COMM_006B_T t
left outer join ${etlvar::ADWDB}.POS_INV_HEADER i
on t.ld_inv_num=i.inv_num 
union all
select
         t.case_id
        ,' ' as sub_case_id_lst
        ,' ' as ukey
        ,&rpt_mth as rpt_mth
        ,t.cm_cust_num
        ,t.cm_subr_num
        ,t.cm_rate_plan_cd as cm_rate_plan_cd
        ,' ' as cm_mthend_status
        ,' ' as lm_cust_num
        ,' ' as lm_subr_num
        ,' ' as lm_rate_plan_cd
        ,date '2999-12-31' as ld_start_date
        ,' ' as ld_inv_num
        ,t.cm_cc_ld_cd as ld_cd
        ,' ' as ld_mkt_cd
        ,date '1900-01-01' as ld_exp_date
        ,date '1900-01-01' as ld_inv_date
        ,t.cm_fm_flg
        ,t.cm_fm_main_subr
        ,t.cm_fm_main_cust
        ,date '1900-01-01' as min_ukey_subr_sw_on_date
        ,' ' as flex_flg
        ,' ' as flex_inv
        ,date '1900-01-01' as flex_ld_start_date
        ,date '1900-01-01' as flex_end_date
        ,t.prnk
        ,date '2999-12-31' as cm_bill_cd_start_date
        ,date '2999-12-31' as lm_bill_cd_end_date
        ,t.cc_last_contact_date
        ,' ' as online_store_flg
        ,t.gz_call_centre_flg
        ,' ' as cm_store_cd
        ,' ' as cm_salesman_cd
        ,' ' as calc_rmk
        ,' ' as sales_type
        ,sysdate as create_ts
        ,' ' as cm_store_json
        ,' 'as cm_salesman_json
        ,'N' as skip_flg
        ,' ' as skip_rmk
        ,0 as rbd_free_mth
        ,0 as rbd_fixed_amt
        ,0 as rbd_hs_subsidy
        ,0 as rbd_overr_contract_mth
        ,' ' as rbd_overr_flg
        ,' ' as buy_out_from
        ,' ' as om_chg_plan_next_mth
        ,date '1900-01-01' om_chg_plan_req_date
        ,1 as remain_contract_mth
        ,1 as paid_mth
        ,t.tcv as tcv
        ,0 as comm
      ,0 as retent_comm
        ,0 as cm_rebate
        ,0 as fin_fm_sim_cnt
        ,' ' as fin_fm_sub_sim_plan_cd
        ,0 as cm_fm_active_sim_cnt
        ,t.cm_cc_rate_plan_rate  as cm_rate_plan_rate
        ,0 as arpu
          ,0 as cm_ld_contract_mth
         ,' ' as prv_case_id
        ,0 as prv_retent_comm
        ,0 as tariff
        ,0 as cm_hs_subsidy
        ,' ' as prv_ld_inv_num
        ,date '1900-01-01' as prv_ld_exp_date
        ,date '1900-01-01' as prv_ld_exp_date_cm
        ,'CC_DUMMY' as comm_type
        ,' ' as comm_channel
        ,t.tcv as prv_tcv
        ,t.tcv as std_tcv
        ,' ' as lm_ld_inv_num
        ,' ' as lm_ld_cd
        ,date '1900-01-01' as lm_ld_exp_date
        ,' ' as cm_port_type
        ,'CC_DUMMY' as cm_cc_line_type
        ,t.cc_comm
        ,'CC_DUMMY' as cc_calc_remark
        ,0 as prv_paid_mth
        ,date '2999-12-31' as prv_comm_mth
        ,0 as prv_tcv_remain
        ,'N' as cm_fm_main_exist_flg
        ,t.cm_cc_comm_date
        ,t.cm_cc_salesman_cd
        ,t.cm_cc_store_cd  
from mig_adw.B_RETENT_UPG_COMM_006C02_T t;
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

