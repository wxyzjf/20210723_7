/opt/etl/prd/etl/APP/RPT/U_OB_PRE_998_RPT/bin> cat master_dev.pl
package etlvar;

$TDUSR = "PRD_ADW_BAT";
$TDPWD = "smc1000";
$ETL_DB = "prd_etl";
$AUTO_DB = "PRD_ETL";
$ETLDB = "PRD_ETL";
$ADWDB = "PRD_ADW";
$BIZDB = "PRD_BIZ_SUMM";
$DB_USER = "prd_etl_bat";
$TMPDB = "PRD_TMP";
$TDDSN="UDSDWDB_ADWETLBAT";
$UTLDB="PRD_ETL_UTL";
$MIGADWDB= "MIG_ADW";
$MIGDB="MIG_ADW";

$ETL_OUTPUT_DIR = "/opt/etl/output";
$ETLSYS = "RPT";
$ETLJOBNAME = "U_OB_PRE_998_RPT";

$SET_ERRLVL_1 = "whenever sqlerror exit 2;\n";
$SET_ERRLVL_2 = "set serveroutput on;\n set sqlblanklines on;\n set autocommit 1;\n set timing on; \n set echo on; \n set sqlprompt ' '; \n set sqlnumber off; \n set define off; \n ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'; \n ";      #Reserved
$SET_ERRLVL_3 = "set echo on;"; #Only used for Auto_Gen when the reference object does not exist




$MINDATE = "1900-01-01";
$MAXDATE = "2999-12-31";


$LOGON_TD=".logon prd_adw_bat,smc1000;";

sub preProcess {
        return 0;
}
sub getTXDate{
        return 0;
}
sub genFirstDayOfMonth{
        return 0;
}

sub postProcess {
        return 0; 
}

###### Test param ####





$F_D_MONTH[0] = "2019-05-01";
