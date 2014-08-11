#!/usr/bin/perl 
#$Rev:: 11                                                 $
#$Author:: bzh                                             $

use lib '/home/bzhang/tools/perlmod';
use ReadFilePerFmt;
use vHist;

@vlst_a=qw(
merchType
merchID
merchCNTRY
Account
tranD90
AuthDec_Ind
Pos_Ind
tranTime
tranAmount
CNP_Ind
IssuerCNTRY
DataSrc
Tran_Type
);
        
%Fmt_a = &ReadFilePerFmt::parseFmtFileForFields(
        '/home/bxz/formats/fmt_MPB2auth_extracted.inc',@vlst_a);

@vl=qw(
merchType
MID_Leading_Letter
MID_Length
MID_Is_Number
MID_Has_no_word
MID_Is_All_Zero
merchCNTRY
Bin
tranD90
AuthDec_Ind
Pos_Ind
tranHour
tranTime
tranAmount
CNP_Ind
IssuerCNTRY
DataSrc
Tran_Type
);

@numeric = qw(
tranAmount
MID_Length
);
@amt = qw(
tranAmount
);

@categorical = qw(
merchType
merchCNTRY
Bin
IssuerCNTRY
);

vHist::setData(\@vl,\@numeric,\@amt,\@categorical);
while (<STDIN>) {
  chomp;
  %data = &ReadFilePerFmt::parseLine ($_, %Fmt_a);
  $data{"Bin"}=substr($data{"Account"},0,6);
  foreach $i (@amt) {$data{$i}=$data{$i}/100;}
  $data{"tranHour"}=substr($data{"tranTime"},0,2);
  
  $mid=$data{"merchID"};
  $data{"MID_Leading_Letter"}=substr($mid,0,1);
  $mid=~s/(^ +| +$)//g;
  $data{"MID_Length"}=length($mid);
  $data{"MID_Is_Number"}=($mid=~/^\d+$/)?1:0;
  $data{"MID_Has_no_word"}=($mid=~/\W/)?1:0;
  $data{"MID_Is_All_Zero"}=($mid=~/^0+$/)?1:0;
  vHist::update(\%data);
}

vHist::printOut();
