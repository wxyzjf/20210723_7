#!/usr/perl5/bin/perl -w
#
# $Header$
#
# $Locker$
#
# $Log$
#
#
#use strict;
use Spreadsheet::XLSX;
use Spreadsheet::ParseExcel::Utility qw(ExcelFmt);
use Date::Manip;
use Encode qw(decode encode);
use Encode qw( from_to );

$xlsfile = $ARGV[0]; 
$txtfile = $ARGV[1];
my $datefmt = 'd-mmm-yy';
open(OUT, ">$txtfile") or die "Cannot open $xlsfile";
$excel = Spreadsheet::XLSX -> new ($xlsfile);
my $sheet= $excel -> {Worksheet}[0];
$sheet -> {MaxRow} ||=$sheet -> {MinRow};
foreach $row ($sheet -> {MinRow} + 1 .. $sheet -> {MaxRow}) {
        $sheet -> {MaxCol} ||= $sheet -> {MinCol};
        foreach $col ($sheet -> {MinCol} .. $sheet -> {MaxCol}) {
                $cell = $sheet -> {Cells} [$row] [$col];
                if (defined $cell) {
                        if ($col == 3){
                                $value = ExcelFmt($datefmt, $cell->{Val});
                                printf OUT ("%s|", "$value");
                        }   else {
                                $value = $cell -> {Val};
                                $value =~ s/&amp;/&/g;
                                printf OUT ("%s|", $value);
                        }
                } else {
                        printf OUT ("%s|", '');
                }
        };
        print OUT "\n";
}










