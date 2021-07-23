/opt/etl/prd/etl/APP/ADW/B_RETENT_UPG_COMM_RPT/bin> cat unload.sql
set head off
set verify off
set trimspool on
set newpage 0
set pagesize 0
set lines 1500
set termout off
set serveroutput off
set feedback off
set echo off
set trimout on
set colsep |
spool 'B_RETENT_UPG_COMM_RPT.csv'
select 
        ',CASE_ID'
        ||',SUB_CASE_ID_LST'
        ||',UKEY'
        ||',RPT_MTH'
        ||',CM_CUST_NUM'
        ||',CM_SUBR_NUM'
        ||',CM_RATE_PLAN_CD'
        ||',CM_MTHEND_STATUS'
        ||',LM_CUST_NUM'
        ||',LM_SUBR_NUM'
        ||',LM_RATE_PLAN_CD'
        ||',LD_START_DATE'
        ||',LD_INV_NUM'
        ||',LD_CD'
        ||',LD_MKT_CD'
        ||',LD_EXP_DATE'
        ||',LD_INV_DATE'
        ||',CM_FM_FLG'
        ||',CM_FM_MAIN_SUBR'
        ||',CM_FM_MAIN_CUST'
        ||',MIN_UKEY_SUBR_SW_ON_DATE'
        ||',FLEX_FLG'
        ||',FLEX_INV'
        ||',FLEX_LD_START_DATE'
        ||',FLEX_END_DATE'
        ||',PRNK'
        ||',CM_BILL_CD_START_DATE'
        ||',LM_BILL_CD_END_DATE'
        ||',CC_LAST_CONTACT_DATE'
        ||',ONLINE_STORE_FLG'
        ||',GZ_CALL_CENTRE_FLG'
        ||',CM_STORE_CD'
        ||',CM_SALESMAN_CD'
        ||',CALC_RMK'
        ||',SALES_TYPE'
        ||',CREATE_TS'
        ||',CM_STORE_JSON'
        ||',CM_SALESMAN_JSON'
        ||',SKIP_FLG'
        ||',SKIP_RMK'
        ||',RBD_FREE_MTH'
        ||',RBD_FIXED_AMT'
        ||',RBD_HS_SUBSIDY'
        ||',RBD_OVERR_CONTRACT_MTH'
        ||',RBD_OVERR_FLG'
        ||',BUY_OUT_FROM'
        ||',OM_CHG_PLAN_NEXT_MTH'
        ||',OM_CHG_PLAN_REQ_DATE'
        ||',REMAIN_CONTRACT_MTH'
        ||',PAID_MTH'
        ||',TCV'
        ||',COMM'
        ||',RETENT_COMM'
        ||',CM_REBATE'
        ||',FIN_FM_SIM_CNT'
        ||',FIN_FM_SUB_SIM_PLAN_CD'
        ||',CM_FM_ACTIVE_SIM_CNT'
        ||',CM_RATE_PLAN_RATE'
        ||',ARPU'
        ||',CM_LD_CONTRACT_MTH'
        ||',PRV_CASE_ID'
        ||',PRV_RETENT_COMM'
        ||',TARIFF'
        ||',CM_HS_SUBSIDY'
from  dual
union all
select 
        CASE_ID
        ||','--||SUB_CASE_ID_LST
        ||','--||UKEY
        ||','||TO_CHAR(RPT_MTH,'YYYY-MM-DD')
        ||','||CM_CUST_NUM
        ||','||CM_SUBR_NUM
        ||','||CM_RATE_PLAN_CD
        ||','||CM_MTHEND_STATUS
        ||','||LM_CUST_NUM
        ||','||LM_SUBR_NUM
        ||','||LM_RATE_PLAN_CD
        ||','||TO_CHAR(LD_START_DATE,'YYYY-MM-DD')
        ||','||LD_INV_NUM
        ||','||LD_CD
        ||','||LD_MKT_CD
        ||','||TO_CHAR(LD_EXP_DATE,'YYYY-MM-DD')
        ||','||TO_CHAR(LD_INV_DATE,'YYYY-MM-DD')
        ||','||CM_FM_FLG
        ||','||CM_FM_MAIN_SUBR
        ||','||CM_FM_MAIN_CUST
        ||','||TO_CHAR(MIN_UKEY_SUBR_SW_ON_DATE,'YYYY-MM-DD')
        ||','||FLEX_FLG
        ||','||FLEX_INV
        ||','||TO_CHAR(FLEX_LD_START_DATE,'YYYY-MM-DD')
        ||','||TO_CHAR(FLEX_END_DATE,'YYYY-MM-DD')
        ||','||PRNK
        ||','||TO_CHAR(CM_BILL_CD_START_DATE,'YYYY-MM-DD')
        ||','||TO_CHAR(LM_BILL_CD_END_DATE,'YYYY-MM-DD')
        ||','||TO_CHAR(CC_LAST_CONTACT_DATE,'YYYY-MM-DD')
        ||','||ONLINE_STORE_FLG
        ||','||GZ_CALL_CENTRE_FLG
        ||','||CM_STORE_CD
        ||','||CM_SALESMAN_CD
        ||','||CALC_RMK
        ||','||SALES_TYPE
        ||','||TO_CHAR(CREATE_TS,'YYYY-MM-DD')
        ||','||CM_STORE_JSON
        ||','||CM_SALESMAN_JSON
        ||','||SKIP_FLG
        ||','||SKIP_RMK
        ||','||RBD_FREE_MTH
        ||','||RBD_FIXED_AMT
        ||','||RBD_HS_SUBSIDY
        ||','||RBD_OVERR_CONTRACT_MTH
        ||','||RBD_OVERR_FLG
        ||','||BUY_OUT_FROM
        ||','||OM_CHG_PLAN_NEXT_MTH
        ||','||TO_CHAR(OM_CHG_PLAN_REQ_DATE,'YYYY-MM-DD')
        ||','||REMAIN_CONTRACT_MTH
        ||','||PAID_MTH
        ||','||TCV
        ||','||COMM
        ||','||RETENT_COMM
        ||','||CM_REBATE
        ||','||FIN_FM_SIM_CNT
        ||','||FIN_FM_SUB_SIM_PLAN_CD
        ||','||CM_FM_ACTIVE_SIM_CNT
        ||','||CM_RATE_PLAN_RATE
        ||','||ARPU
        ||','||CM_LD_CONTRACT_MTH
        ||','||PRV_CASE_ID
        ||','||PRV_RETENT_COMM
        ||','||TARIFF
        ||','||CM_HS_SUBSIDY
from mig_adw.B_RETENT_UPG_COMM_006B_T
where rpt_mth = date '2019-02-01';
spool off;
quit;

