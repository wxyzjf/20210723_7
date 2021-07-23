/opt/etl/output/RPT/Z_IDP_MAPR_UNLOAD> cat /home/adwbat/script/RUN_EA_JOB.ksh
#!/usr/bin/ksh
#
# Touch a file in EA "Receive" Directory to Start an EA Job
#
# Input parameters:
#       1. Job_Name
#       2. Number of Days Before
#

. /opt/etl/prd/etl/etc/.pre_profile 2>&1

CURR_DATE=`date '+%Y%m%d'`

LOG_PATH=/home/adwbat/log
LOG_NAME=RUN_EA_JOB.LOG
LOG_FILE=${LOG_PATH}/${LOG_NAME}.${CURR_DATE}

echo $LOG_FILE


if [ ! "$1" ]; then
   echo "Job Name is missing \n" >> $LOG_FILE
   exit 1 
fi


JOB_NAME=$1
TX_DATE="`date '+%Y%m%d'`"



if [ "$2" ]; then
  TX_DATE=`perl -e 'use Date::Manip; $date = DateCalc("today", "- '$2' days", \$err); print &UnixDate($date, "'%Y%m%d'");'`
fi


echo "`date`" >> $LOG_FILE
echo "  EA Job Name: $JOB_NAME" >> $LOG_FILE
echo "  Tx Date: $TX_DATE" >> $LOG_FILE


touch /opt/etl/prd/etl/DATA/receive/dir.$JOB_NAME$TX_DATE

if [ $? -ne 0 ]; then
  echo "!! Failed !! \n" >> $LOG_FILE
  exit 1
fi


echo "Done \n" >> $LOG_FILE



##-----------------##
##    HOUSEKEEP    ##
##-----------------##

find $LOG_PATH -name '${LOG_NAME}*' -mtime +100 -exec rm -f {} \;


exit

