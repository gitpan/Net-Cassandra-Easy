#!/usr/bin/perl

use warnings;
use strict;
use Data::Dumper;

use strict;

use Net::Cassandra::Easy;

use Test::More tests => 41;
use Data::Dumper;

local $Data::Dumper::Indent = 0;
local $Data::Dumper::Terse = 1;

my $debug = $Net::Cassandra::Easy::DEBUG = scalar @ARGV;

my ($server, $keyspace, $family) = @ENV{qw/CASSANDRA_SERVER CASSANDRA_KEYSPACE CASSANDRA_FAMILY/};

my $port = 9160;

my $live = 1;

if ($server)
{
    if ($server =~ m/^(.*):(\d+)$/)
    {
	$server = $1;
	$port = $2;
    }
}
else
{
    print "Sorry but you have to provide a Cassandra server ('host' or 'host:port') in the CASSANDRA_SERVER environment variable.\n";
    $live = 0;
}

unless ($keyspace)
{
    print "Sorry but you have to provide a Cassandra keyspace in the CASSANDRA_KEYSPACE environment variable.\n";
    $live = 0;
}

unless ($family)
{
    print "Sorry but you have to provide a Cassandra supercolumn family (LongType) in the CASSANDRA_FAMILY environment variable.\n";
    $live = 0;
}

SKIP: {
    skip 'Not configured for testing, see test.pl', 41 unless $live;
};

exit unless $live;

my %params = (
	      get =>
	      {
	       fail => [
			[$keyspace, ],		 # no parameters
			[$keyspace, 'huh'],		 # invalid rows
			[$keyspace, {}],		 # invalid rows
			[$keyspace, []],		 # no rows
			[$keyspace, [qw/processes/]], # no family
			[$keyspace, [qw/processes/], family => $family], # no byoffset or byname or bylong
			[$keyspace, [qw/processes/], family => $family, byoffset => 'hello'], # byoffset has to be an array ref
			[$keyspace, [qw/processes/], family => $family, bylong => 'hello'], # bylong has to be an array ref
			[$keyspace, [qw/processes/], family => $family, byname => 'hello'], # byname has to be an array ref
			[$keyspace, [qw/processes/], family => $family, byoffset => {}, byname => []], # byname and byoffset can't both be specified
			[$keyspace, [qw/processes/], family => $family, bylong => [], byname => []], # bylong and byname can't both be specified
			[$keyspace, [qw/processes/], family => $family, byoffset => {}, bylong => []], # byname and byoffset can't both be specified
			[$keyspace, [qw/processes/], family => $family, byoffset => { start => '1024', startlong => '1024', count => 100}], # in byoffset, start and startlong can't both be specified
			[$keyspace, [qw/processes/], family => $family, byoffset => { finish => '1024', finishlong => '1024', count => 100}], # in byoffset, finish and finishlong can't both be specified
		       ],
	       good => [
			[$keyspace, [qw/processes/], family => $family, byoffset => { count => 2 }], # first 2 supercolumns
			[$keyspace, [qw/processes/], family => $family, byoffset => { count => -2 }], # last 2 supercolumns
			[$keyspace, [qw/processes/], family => $family, byoffset => { start => '10245678', count => 1}], # first 1 supercolumns starting at 10245678 as a 8-byte name
			[$keyspace, [qw/processes/], family => $family, byoffset => { startlong => '1024', count => 1}], # first 1 supercolumns starting at 1024 as a 8-byte LongType
			[$keyspace, [qw/processes/], family => $family, byoffset => { startlong => '10', finishlong => '1024', count => 1}], # first 1 supercolumns starting at 10 and finishing at 1024 as a 8-byte LongType
			[$keyspace, [qw/processes/], family => $family, byoffset => { start => '10203040', finishlong => '1024', count => -1}], # last 1 supercolumns starting at 10203040 as a 8-byte name and finishing at 1024 as a 8-byte LongType
			[$keyspace, [qw/processes/], family => $family, byname => [qw/hello!!! goodbye!/]], # gets these supercolumns as 8 bytes each
			[$keyspace, [qw/processes/], family => $family, bylong => [0,1,2, "12345678901234"]], # gets supercolumns with LongType values = 0, 1, 2, 12345678901234
			['Keyspace1', [qw/processes/], standard => 1, family => 'Standard1'], # gets all the columns in this column family (non-super)
		       ],
	      },

	      mutate =>
	      {
	       fail => [
			[$keyspace, ],		    # no parameters
			[$keyspace, 'huh'],		    # invalid rows
			[$keyspace, {}],		    # invalid rows
			[$keyspace, []],		    # no rows
			[$keyspace, [qw/processes/]],    # no family
			[$keyspace, [qw/processes/], family => $family ], # no deletions or insertions
			[$keyspace, [qw/processes/], family => $family, insertions => 'hello'], # invalid insertions
			[$keyspace, [qw/processes/], family => $family, deletions => 'goodbye'], # invalid deletions
			[$keyspace, [qw/processes/], family => $family, deletions => []], # must be hash reference
			[$keyspace, [qw/processes/], family => $family, insertions => {}, deletions => {} ], # nothing to do
			[$keyspace, [qw/processes/], family => $family, insertions => { testing => 123 } ], # fail to insert Columns into a super column family
			[$keyspace, [qw/processes/], family => $family, deletions => { byname => 'hello!!!' } ], # byname argument should be an array
			[$keyspace, [qw/processes/], family => $family, deletions => { byoffset => { count => 1 } } ], # delete the first SuperColumn, fails because Deletions don't support it yet
		       ],
	       good => [
			[$keyspace, [qw/processes/], family => $family, insertions => { 'hello!!!' => { testing => 123 } } ], # insert SuperColumn named 'hello!!!' with one Column
			[$keyspace, [qw/processes/], family => $family, insertions => { Net::Cassandra::Easy::pack_decimal(0) => { testing => 123 } } ], # insert SuperColumn named 0 (as a long with 8 bytes) with one Column
			[$keyspace, [qw/processes/], family => $family, deletions => { byname => ['hello!!!'] } ], # delete SuperColumn named 'hello!!!'
			[$keyspace, [qw/processes/], family => $family, deletions => { bylong => [123] } ], # delete SuperColumn named 123
			['Keyspace1', [qw/processes/], family => 'Standard1', insertions => { testing => 123 } ], # insert Columns into a non-super column family
		       ],
	      },
	     );

foreach my $method (qw/mutate get/)
{
    foreach my $good (@{$params{$method}->{good}})
    {
	my $c = Net::Cassandra::Easy->new(server => $server, port => $port, keyspace => shift @$good, credentials => { none => 1 });
	$c->connect();

	my $result;
	eval
	{
	    $result = $c->$method(@$good);
	};

	if ($@)
	{
	    warn Dumper($@);
	}

	print Dumper($result) if $debug;

	ok(!$@, "$method good: " . Dumper($good));
    }

    foreach my $fail (@{$params{$method}->{fail}})
    {
	my $c = Net::Cassandra::Easy->new(server => $server, port => $port, keyspace => shift @$fail, credentials => { none => 1 });
	$c->connect();
	
	eval
	{
	    $c->$method(@$fail);
	};

	if ($@)
	{
	    warn Dumper($@) if $debug;
	}

	ok($@, "$method fail: " . Dumper($fail) );
    }
}
