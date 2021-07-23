#
use Spreadsheet::ParseExcel;


$xlsfile = $ARGV[0];

my $oExcel = new Spreadsheet::ParseExcel;
my $oBook = $oExcel->Parse($xlsfile);

my($iRow, $iCol, $oWrkSheet, $oWrkCell);

my $iSheet = 0;

$oWrkSheet = $oBook->{Worksheet}[$iSheet];


        for ($iRow = 0; $iRow <= $oWrkSheet->{MaxRow}; $iRow++) {
                
                last if not defined $oWrkSheet->{Cells}[$iRow][0] and 
                not defined $oWrkSheet->{Cells}[$iRow][1];
                if ($iRow == 0) {
                        next;
                        }
                if ($iRow == 1) {
                        next;
                        }
                if ($iRow == 2) {
                        next;
                        }
                if ($iRow == 3) {
                        next;
                        }
                print "BT|";
    
        for ($iCol = 0; $iCol <= 26; $iCol++) {
                $oWrkCell = $oWrkSheet->{Cells}[$iRow][$iCol];
                if (defined $oWrkCell) {
                                $cell = $oWrkCell->Value;
                                chomp $cell;

                        #print $cell;
                }        
                if ($iCol == 7) {
                        print $cell;
                        print "|";
                        }                
                if ($iCol == 0) {
                        print $cell;
                        print "|";
                        }
                if ($iCol == 11) {
                        print $cell;
                        print "|";
                        }
                if ($iCol == 4) {
                        print $cell;
                        print "|";
                        }
                if ($iCol == 22) {
                        print $cell;
                        }
        }
                print "\n";
        }

