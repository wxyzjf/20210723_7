/opt/etl/prd/etl/APP/RPT/U_RELAY42_TX_GEN_RPT/bin> cat master_dev.pl
package etlvar;
$TDUSR = "PRD_ADW_BAT";
$TDPWD = "smc1000";
$ETL_DB = "prd_etl";
$AUTO_DB = "PRD_ETL";
$ETLDB = "PRD_ETL";
$ADWDB = "PRD_ADW";
$BIZDB = "PRD_BIZ_SUMM";
$DB_USER = "prd_etl_bat";
$TMPDB = "MIG_ADW";
$TDDSN="UDSDWDB_ADWETLBAT";
$UTLDB="PRD_ETL_UTL";
$MIGADWDB= "MIG_ADW";
$MIGDB="MIG_ADW";
$BIZVW ="prd_biz_summ_vw";
$BIZDB = "PRD_BIZ_SUMM";
$DSSVR = $ENV{"DSSVR"};
$DSPROJECT = $ENV{"DSPROJECT"};
$ulog = q(2>&1);
$TDSVR = $ENV{"TDSVR"};
$ETLJOBNAME = "U_RELAY42_TX_GEN_RPT";
$ETLPATH = $ENV{"ETLPATH"};
$ETL_TMP_DIR_DS = "${ETLPATH}/tmp";
$ENABLE_FTP = "Y";
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
$year += 1900;
$mon = sprintf("%02d", $mon+1);
$mday = sprintf("%02d", $mday);
$hour = sprintf("%02d",$hour);
$min = sprintf("%02d",$min);
$sec = sprintf("%02d",$sec);
$LOADTIME = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";
$TXDATE = "2019-07-29";
$TXMONTH = "201907";
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
$F_D_MONTH[0] = "2019-05-01";
