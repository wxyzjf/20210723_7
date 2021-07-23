/opt/etl/prd/etl/preprocess/USR/script> cat x_shkdp_rpt.pl
#!/usr/perl5/bin/perl -w
#
# Usage: xls2txt.pl <input_excel_file> <output_text_file>
#
#use Spreadsheet::ParseExcel;

use Spreadsheet::XLSX;

$xlsfile = $ARGV[0];
$outfile= $ARGV[1];

my $workbook = Spreadsheet::XLSX -> new ($xlsfile);

my($iRow, $iCol, $oWrkSheet, $oWrkCell);
$Col_Start = 0;
$Col_End = 12;

#@Col_Skip = (1);


open(OUT, ">$outfile")or die "Could not open file '$outfile' $!";

$iSheet = 0;

    $oWrkSheet = $workbook->{Worksheet}[$iSheet];

    for ($iRow = 1; $iRow <= $oWrkSheet->{MaxRow}; $iRow++)
    {
        @oWrkCell = ();
        for ($iCol = $Col_Start; $iCol <= $Col_End; $iCol++)
        {
            next if grep { $_ eq $iCol} @Col_Skip;
                        $tmp_cell = $oWrkSheet->{Cells}[$iRow][$iCol];
            if (not defined $tmp_cell){
                                push @oWrkCell, '';
                        }
                        else{
                                chomp $tmp_cell;


                                $tmp_str = $tmp_cell->Value;
                                if ($tmp_str =~/[\$]/){
                                        $tmp_str =~ s/\$//;
                                        $tmp_str =~ s/\,//;
                                        $tmp_str =~ s/\ //;
                                }
                                 push @oWrkCell, $tmp_str;
                   }
        }
        print OUT join ",", @oWrkCell;
        print OUT "\n";
    }

close (OUT);

exit 0;
