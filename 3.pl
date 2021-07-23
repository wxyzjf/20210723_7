/opt/etl/prd/etl/preprocess/USR/script> cat x_bm_comm_rpt_ict_ref.pl
#
use Spreadsheet::ParseExcel;


$xlsfile = $ARGV[0];

my $oExcel = new Spreadsheet::ParseExcel;
my $oBook = $oExcel->Parse($xlsfile);

my($iRow, $iCol, $oWrkSheet, $oWrkCell);

my $iSheet = 2;

$oWrkSheet = $oBook->{Worksheet}[$iSheet];

        for ($iRow = 0; $iRow <= $oWrkSheet->{MaxRow}; $iRow++) {
                last if not defined $oWrkSheet->{Cells}[$iRow][0] and 
                not defined $oWrkSheet->{Cells}[$iRow][1];
    
        for ($iCol = 0; $iCol <= 40; $iCol++) {
                $oWrkCell = $oWrkSheet->{Cells}[$iRow][$iCol];
                if (defined $oWrkCell) {
                                $cell = $oWrkCell->Value;
                                chomp $cell;
                                #$cell =~ s/,//;
                                #$cell =~ s/\s+$//g;
                        print $cell;
                } 
                if ($iCol != 40) {
                        print ",";
                        }
        }
                print "\n";
        }


