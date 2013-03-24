#!/usr/bin/perl
my $prev = -1;
while (<>) {
  if ($_ < $prev) {
	print "FAIL\n";
	exit(-1);
   }
   $prev = $_;
}
print "WIN\n";
exit(0);
