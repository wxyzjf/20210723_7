
/opt/etl/prd/etl/preprocess/USR/script> cat D_BM_STAFF_LIST.pl
#!/usr/bin/perl -w

use strict;
use Spreadsheet::ParseExcel;
use Spreadsheet::XLSX;

my $xlsfile = $ARGV[0];

my $parser   = Spreadsheet::ParseExcel->new();
my $workbook = Spreadsheet::XLSX -> new ($xlsfile);

if ( !defined $workbook ) {
    die $parser->error(), ".\n";
}

for my $worksheet ( $workbook->worksheets() ) {

    my ( $row_min, $row_max ) = $worksheet->row_range();
    my ( $col_min, $col_max ) = $worksheet->col_range();

    for my $row ( $row_min .. $row_max ) {
        for my $col ( $col_min .. $col_max ) {

            my $cell = $worksheet->get_cell( $row, $col );
            next unless $cell;
            
            print $cell->value();
            if ( $col < $col_max ){
                print ",";
            }
        }
        print "\n";
    }
}




