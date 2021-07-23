/opt/etl/prd/etl/APP/RPT/U_OB_PRE_998_RPT/bin> cat u_ob_pre_998_rpt0010.pl
#!/usr/bin/perl
######################################################
#   Purpose:
#
#
######################################################

my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;

#my $ETLVAR = "/opt/etl/prd/etl/APP/RPT/U_OB_PRE_998_RPT/bin/master_dev.pl";
require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here


my $ENV;


my $OUTPUT_FILE_PATH, $DEST_DIR,$OUTPUT_FILE_NAME_1;
#OUTPUT_FILE_NAME_2,OUTPUT_FILE_NAME_3,OUTPUT_FILE_NAME_4,OUTPUT_FILE_NAME_5,OUTPUT_FILE_NAME_6;

my $FTP_FROM_HOST,$FTP_FROM_USERNAME,$FTP_FROM_PASSWORD;
my $FTP_TO_HOST,$FTP_TO_PORT,$FTP_TO_USERNAME,$FTP_TO_PASSWORD,$FTP_TO_DEST_PATH;

my $PROCESS_DATE;

my $comChanType;
my $head_value;
my $sql_head_value;

##################################################################################################################################
##################################################################################################################################
##################################################################################################################################


sub initParam{

    $ENV = $ENV{"ETL_ENV"};

use POSIX;

    use Date::Manip;
    use DBI;

    my $PROCESS_DATE = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");
    my $FILE_DATE= &UnixDate(DateCalc("${etlvar::F_D_MONTH[0]}","-1 months",\$err),'%Y%m%d');



my $year_month_day=strftime("%Y%m%d",localtime());
    # ------------------------------------------------------------------#
    #  Please define the parameters for this job below.                 #
    # ------------------------------------------------------------------#

  $OUTPUT_FILE_PATH = ${etlvar::ETL_OUTPUT_DIR}."/".${etlvar::ETLSYS}."/".${etlvar::ETLJOBNAME};
#print("$OUTPUT_FILE_PATH");
#$OUTPUT_FILE_PATH = "/opt/etl/prd/etl/APP/RPT/U_OB_PRE_998_RPT/bin";
#$OUTPUT_FILE_PATH = "/opt/etl/output/RPT/U_OB_PRE_998_RPT/bin";
print("$OUTPUT_FILE_PATH");
        if (! -d $OUTPUT_FILE_PATH) {
            system("mkdir ${OUTPUT_FILE_PATH}");
        }

    ##system("rm -f ${OUTPUT_FILE_PATH}/FES_PROD_PRICE_HIST_".${etlvar::TXDATE}.".txt*");

   # $OUTPUT_FILE_NAME_1 = "HK_RATE_PLAN_REF_".$PROCESS_DATE.".txt";
$OUTPUT_FILE_NAME_1 = "ob_pre_998_".$year_month_day.".txt";

#    if ($ENV eq "DEV")
#    {
#        ##  DEVELOPMENT  ##
#
#        # PUT action
#        $FTP_TO_HOST = "";                                          # Please define
#        $FTP_TO_PORT = "";                                          # Please define
#        $FTP_TO_USERNAME = "";                                      # Please define
#        $FTP_TO_PASSWORD = "";                                      # Please define
#        $FTP_TO_DEST_PATH = "";                                     # Please define
#
#        # GET action   (ONLY  FOR  DEVELOPMENT)
#        $FTP_FROM_HOST = "${etlvar::DSSVR}";
#        $FTP_FROM_USERNAME = "${etlvar::DSUSR}";
#        $FTP_FROM_PASSWORD = "${etlvar::DSPWD}";
#    }
#    else
#    {
#        ##  PRODUCTION  ##
#
#       $FTP_TO_HOST = "ftpsvc01";                                          # Please define
#        $FTP_TO_PORT = "2026";                                          # Please define
#        $FTP_TO_USERNAME = "smc/dw_ftp";                                      # Please define
#        $FTP_TO_PASSWORD = "dw000000";                                      # Please define
#        $FTP_TO_DEST_PATH = "/world/teamwork/Tracking_Rpt/Comm_Cap_Amort";                                     # Please define
#    }

$FTP_TO_HOST = "sftp://172.20.93.249";
$FTP_TO_PORT = "22";
$FTP_TO_USERNAME = "dwftp";
$FTP_TO_PASSWORD = "dwftp";
$FTP_TO_DEST_PATH = "/export/home/dwftp/op_out";
}

##################################################################################################################################
##################################################################################################################################
##################################################################################################################################
sub runSQLPLUS_EXPORT{

my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}/u_ob_pre_998_rpt_0010_sqlcmd.sql";
#my $SQLCMD_FILE="/opt/etl/prd/etl/APP/RPT/U_OB_PRE_998_RPT/bin/u_ob_pre_998_rpt.sql";
open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;
print SQLCMD<<ENDOFINPUT;

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

SPOOL '${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1}';
SELECT s.subr_num 
  from ${etlvar::ADWDB}.nsp_subr_info_hist s
,      ${etlvar::ADWDB}.nsp_subr_email e 
WHERE s.subr_num = e.subr_num 
  and s.cust_num = e.cust_num 
  and end_date = to_date('${etlvar::MAXDATE}' , 'YYYY-MM-DD') 
  and wsm_promotion ='N' 
  and subr_stat_cd ='OK';

SPOOL OFF;
COMMIT;
quit
ENDOFINPUT
  close(SQLCMD);
  print("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  my $ret = system("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  if ($ret != 0)
  {
    return (1);
  }
}
##################################################################################################################################
##################################################################################################################################
##################################################################################################################################



# Get the file from "bill02"
sub getFile{

    print("\n\n\n");
    print("#####################################\n");
    print("#  GET FILE  (FROM  bill02)\n");
    print("#####################################\n");


    use Net::FTP;
    use File::Copy;


    my $ftp_return = 0;


##  CONNECT TO HOST
    $ftp = Net::FTP->new(${FTP_TO_HOST}, Port=>${FTP_TO_PORT})
        or die "Cannot connect to ${FTP_TO_HOST}: $@";

##  LOGIN
    $ftp->login()       ##("$FTP_FROM_USERNAME","$FTP_FROM_PASSWORD")
        or die "Cannot login ", $ftp->message;

##  CHANGE REMOTE DIRECTORY
    $ftp->cwd(${FTP_TO_DEST_PATH})
        or die "Cannot change working directory ", $ftp->message;


##  CHANGE TRANSFER MODE  ( ascii / binary )
    $ftp->binary
        or die "Cannot change transfer mode ", $ftp->message;


##  TRANSFER FILES
   ## @file_list = $ftp->ls("${OUTPUT_FILE_PATH}/*.txt");
     @file_list = $ftp->ls("${OUTPUT_FILE_PATH}/*.gz");

    foreach $file (@file_list)
    {
        print("Transfer $file ... ");

        $ftp->put($file)
            or die "Failed! ", $ftp->message;

        print("Done \n");
    }


##  DISCONNECT
    $ftp->quit;


    return $ftp_return;
}



##################################################################################################################################
##################################################################################################################################
##################################################################################################################################



# Process file
sub processFile{

    print("\n\n\n");
    print("#####################################\n");
    print("#  PROCESSING FILES\n");
    print("#####################################\n");


    my $process_return = 0;


##  REMOVE HEADER FROM OUTPUT FILE
    @file_list = <$OUTPUT_FILE_PATH/*.txt>;

    foreach $file (@file_list)
    {
        print("Remove header from $file ... ");

        $process_return = system("cp ${file} ${file}.TMP");
        # $process_return = system("sed '1,2d' ${file} > ${file}.TMP");

        if ($process_return == 0)
            {
                $process_return = system("mv -f ${file}.TMP ${file}");
            }

        if ($process_return == 0)
            {
                print("Done. \n");
            }
        else
            {
                print("Failed! \n");
                exit 9;
            }
    }


    return $process_return;
}

#sendFile
sub sendFile{
    print("\n\n\n");
    print("#####################################################################\n");
    print("#  SEND FILE TO USER  ($FTP_TO_HOST $FTP_TO_PORT:$FTP_TO_DEST_PATH)  \n");
    print("#####################################################################\n");
system("lftp $FTP_TO_HOST -p $FTP_TO_PORT -u $FTP_TO_USERNAME,$FTP_TO_PASSWORD<<FOftp
        set ssl:verify-certificate no
        set ftp:ssl-allow true
        set ftp:ssl-force false
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        cd $FTP_TO_DEST_PATH
        lcd $OUTPUT_FILE_PATH
        put $OUTPUT_FILE_NAME_1
        quit
FOftp
");
        my $RET_CODE = $? >> 8;
        if ($RET_CODE != 0){
                return 1;
print("ret=1");        
}else{
 print("ret=0");              
 return 0;
        }
}

sub uploadFile{

    print("\n\n\n");
    print("#########################################\n");
    print("#  UPLOAD FILE TO SERVER ($FTP_TO_HOST)  \n");
     print("#########################################\n");
    use Net::FTPSSL;
    my $ftp_return = 0;

##  CONNECT TO HOST
    $ftp = Net::FTPSSL->new(${FTP_TO_HOST}, Port=>${FTP_TO_PORT},Encryption => 'E',Debug => 1)
             or $ftp_return = 1;     ##or die "Cannot connect to ${FTP_TO_HOST}: $@";

        if ($ftp_return == 1) {
            print("Cannot connect to server \n");
            return $ftp_return;
        }

##  LOGIN
    print("$FTP_TO_USERNAME-$FTP_TO_PASSWORD"); 
    $ftp->login("$FTP_TO_USERNAME","$FTP_TO_PASSWORD")
        or $ftp_return = 2;     ##or die "Cannot login ", $ftp->message;

        if ($ftp_return == 2) {
            print("Cannot login to server \n");
            return $ftp_return;
        }


##  CHANGE REMOTE DIRECTORY
    $ftp->cwd(${FTP_TO_DEST_PATH})
        or $ftp_return = 3;     ##or die "Cannot change working directory ", $ftp->message;

        if ($ftp_return == 3) {
            print("Cannot change working directory \n");
            return $ftp_return;
        }

##  TRANSFER FILES
    @file_list = <$OUTPUT_FILE_PATH/$OUTPUT_FILE_NAME_1>;

    foreach $file (@file_list)
    {
        print("Transfer $file ... ");
        $ftp->put($file)
            or $ftp_return = 5;     ##or die "Failed! ", $ftp->message;

            if ($ftp_return == 5) {
                print("Failed! \n");
                return $ftp_return;
            }

        print("Done \n");
    }


##  DISCONNECT
    $ftp->quit;


    return $ftp_return;
}


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################
##sub uploadFile{
##
##    print("\n\n\n");
##    print("#########################################\n");
##    print("#  UPLOAD FILE TO SERVER ($FTP_TO_HOST)  \n");
##    print("#########################################\n");
##
##
##    use Net::FTP;
##    use File::Copy;
##
##    if (${etlvar::ENABLE_FTP} eq "N") {
##      print ("FTP is disabled in non-production environment!\n");
##      return 0;
##    }
##
##
##    my $ftp_return = 0;
##
##
####  CONNECT TO HOST
##    $ftp = Net::FTP->new(${FTP_TO_HOST}, Port=>${FTP_TO_PORT})
##        or $ftp_return = 1;     ##or die "Cannot connect to ${FTP_TO_HOST}: $@";
##
##        if ($ftp_return == 1) {
##            print("Cannot connect to server \n");
##            return $ftp_return;
##        }
##
####  LOGIN
##    $ftp->login()       ##("$FTP_TO_USERNAME","$FTP_TO_PASSWORD")
##        or $ftp_return = 2;     ##or die "Cannot login ", $ftp->message;
##
##        if ($ftp_return == 2) {
##            print("Cannot login to server \n");
##            return $ftp_return;
##        }
##
##
####  CHANGE REMOTE DIRECTORY
##    $ftp->cwd(${FTP_TO_DEST_PATH})
##        or $ftp_return = 3;     ##or die "Cannot change working directory ", $ftp->message;
##
##        if ($ftp_return == 3) {
##            print("Cannot change working directory \n");
##            return $ftp_return;
##        }
##
####  CHANGE TRANSFER MODE  ( ascii / binary )
##    $ftp->binary
##        or $ftp_return = 4;     ##or die "Cannot change transfer mode ", $ftp->message;
##
##        if ($ftp_return == 4) {
##            print("Cannot change transfer mode \n");
##            return $ftp_return;
##        }
##
##
####  TRANSFER FILES
##
###   @file_list = <$OUTPUT_FILE_PATH/*.dat>;
##   ## @file_list = <$OUTPUT_FILE_PATH/$OUTPUT_FILE_NAME_1>;
##      @file_list = <$OUTPUT_FILE_PATH/*.gz>;
##
##    foreach $file (@file_list)
##    {
##        print("Transfer $file ... ");
##          $ftp->put($file)
##            or $ftp_return = 5;     ##or die "Failed! ", $ftp->message;
##
##            if ($ftp_return == 5) {
##                print("Failed! \n");
##                return $ftp_return;
##            }
##
##        print("Done \n");
##    }
##
##
####  DISCONNECT
##    $ftp->quit;
##
##
##    return $ftp_return;
##}
##################################################################################################################################
##################################################################################################################################
##################################################################################################################################

sub uploadFile_2{

    print("\n\n\n");
    print("#########################################\n");
    print("#  UPLOAD FILE TO SERVER ($FTP_TO_HOST_2)  \n");
    print("#########################################\n");

    my $process_ret = 0;
    my $file_list_y1 = <${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1}.gz>;

    print("UPLOAD FILE TO SFTP SERVER: $file_list_y1 \n");
    $process_ret = system("/opt/etl/prd/etl/APP/RPT/U_IFR_FES_PROD_PRICE_HIST/bin/sftp_example.exp ${file_list_y1}");

    return $process_ret;
}

##############################################################################################################################
##############################################################################################################################
##############################################################################################################################
sub uploadFile_3{

    print("\n\n\n");
    print("#########################################\n");
    print("#  UPLOAD FILE TO SERVER ($FTP_TO_HOST_2)  \n");
    print("#########################################\n");

    my $process_ret = 0;
    my $file_list_y1 = <${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1}.complete>;

    print("UPLOAD FILE TO SFTP SERVER: $file_list_y1 \n");
    $process_ret = system("/opt/etl/prd/etl/APP/RPT/U_IFR_FES_PROD_PRICE_HIST/bin/sftp_example.exp ${file_list_y1}");

    return $process_ret;
}
##############################################################################################################################
##############################################################################################################################
##############################################################################################################################

# Send the file to user
sub copyFile{

    print("\n\n\n");
    print("############################################################\n");
    print("#   COPY FILES TO DIRECTORY:   ${DEST_DIR} \n");
    print("############################################################\n");


    my $ftp_return = 0;

    system("cp ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1} ${DEST_DIR}/${OUTPUT_FILE_NAME_1}") == 0 or die "Copy ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1} ... Failed! \n";


    print("Done. \n");

    return $ftp_return;
}



##################################################################################################################################
##################################################################################################################################
##################################################################################################################################


sub EmailUser{

    print("\n\n\n#####################################\n");
    print("#  Email user\n");
    print("#####################################\n");

    my $TOLIST, $CCLIST, $BCCLIST, $SUBJECT, $ATTACH;


    if ($ENV eq "DEV")
    {
        $TOLIST = "".q(@)."smartone123.com456";
        $CCLIST = "";
        $BCCLIST = "";
        $ATTACH = "";
    }
    else
    {   
        ##$TOLIST  = "kevin_ou".q(@)."smartone.com int_Sara_Luo".q(@)."smartone.com";
        ##$TOLIST  = "joe_chan".q(@)."smartone.com int_Sara_Luo".q(@)."smartone.com";        
        $TOLIST  = "Eloise_Wu".q(@)."smartone.com Eloise_Wu".q(@)."smartone.com";
        $SUBJECT = "HK RATE_PLAN_REF ";
        $ATTACH = "-a ".${OUTPUT_FILE_PATH}."/".${OUTPUT_FILE_NAME_1};
    }


    my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}' ${TOLIST} ${ATTACH}");
    unless ($rc){
            print "Cound not invoke mutt command\n";
            return -1;
    }

    print EMAIL_EOF<<ENDOFINPUT;

Please check attachment.

ENDOFINPUT

    close(EMAIL_EOF);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }

}


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################



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

## Disable the Perl buffer, Print the log message immediately
$| = 1;


initParam();


my $ret = 0;


##################################################################################

# RUN SQL and EXPORT FILE
if ($ret == 0)
{
    $ret = runSQLPLUS_EXPORT();
}


##################################################################################

# REMOVE FILE HEADER
#if ($ret == 0)
#{
#   $ret = processFile();
#}

##system("gzip -f ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1}");

##################################################################################


#if ($ret == 0)
#{
     # UPLOAD FILE
#     $ret = uploadFile();
#}
if ($ret == 0)
{
     $ret = sendFile();
}


##################################################################################


# COPY FILES TO DESTINATION DIRECTORY
#if ($ret == 0)
#{
#  # $ret = copyFile();
#}


##################################################################################


## EMAIL  ALERT  TO  USER
if ($ret == 0){

  $ret = EmailUser();

    if ($ret != 0){
        exit 2;
    }
}

my $post = etlvar::postProcess();

exit($ret);














