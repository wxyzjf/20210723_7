perl /opt/etl/prd/etl/APP/USR/X_BM_COMM_RPT_ICT_REF/bin/reload/x_bm_comm_rpt_ict_ref.pl /opt/etl/prd/etl/APP/USR/X_BM_COMM_RPT_ICT_REF/bin/reload/in/2.xls > /opt/etl/prd/etl/APP/USR/X_BM_COMM_RPT_ICT_REF/bin/reload/out/2.csv





        for ($iRow = 0; $iRow <= $oWrkSheet->{MaxRow}; $iRow++) {
                last if not defined $oWrkSheet->{Cells}[$iRow][0] and
                not defined $oWrkSheet->{Cells}[$iRow][1];
                #if ($iRow == 0) {
                #        next;
                #        }
                for ($iCol = 0; $iCol <= 40; $iCol++) {
                        $oWrkCell = $oWrkSheet->{Cells}[$iRow][$iCol];
                         if (defined $oWrkCell) {
                                $cell = $oWrkCell->Value;
                                chomp $cell;
                          }
                         if ($iCol <=6 or ($iCol >=13 and $iCol <=27)) {
                                print $cell;
                                print ",";
                         }
#                        if ($iCol == 28) {
#                               print ",";
#                        }
                }
                print "\n";
        }





BILL_CD,
CAT,
SUB_CAT,
DTL_CAT,
TYPE1,
TYPE2,
TYPE3,
ONEOFF_COST,
ADD_ONEOFF_COST,
MONTHLY_COST,
HKBN_MTHLY_12,
HKBN_MTHLY_24,
HKBN_MTHLY_36,
HKBN_ONEOFF,
WTT_MTHLY_12,
WTT_MTHLY_24,
WTT_MTHLY_36,
WTT_ONEOFF,
HGC_MTHLY_12,
HGC_ONEOFF_12,
HGC_MTHLY_24,
HGC_MTHLY_36

CREATE TABLE MIG_ADW.X_BM_COMM_RPT_ICT_REF
(
  BILL_CD          VARCHAR2(50 BYTE)            DEFAULT ' '                   NOT NULL,
  CAT              VARCHAR2(50 BYTE)            DEFAULT ' '                   NOT NULL,
  SUB_CAT          VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  DTL_CAT          VARCHAR2(500 BYTE)           DEFAULT ' '                   NOT NULL,
  TYPE1            VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  TYPE2            VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  TYPE3            VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  ONEOFF_COST      VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  ADD_ONEOFF_COST  VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  MONTHLY_COST     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HKBN_MTHLY_12    VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HKBN_MTHLY_24    VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HKBN_MTHLY_36    VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HKBN_ONEOFF      VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  WTT_MTHLY_12     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  WTT_MTHLY_24     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  WTT_MTHLY_36     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  WTT_ONEOFF       VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HGC_MTHLY_12     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HGC_ONEOFF_12    VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HGC_MTHLY_24     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  HGC_MTHLY_36     VARCHAR2(100 BYTE)           DEFAULT ' '                   NOT NULL,
  CREATE_TS        TIMESTAMP(0)                 DEFAULT TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') NOT NULL,
  REFRESH_TS       TIMESTAMP(0)                 DEFAULT TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') NOT NULL
)


















