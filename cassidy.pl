#!/usr/bin/perl

use lib './lib';

use Data::Dumper;
use Sys::Hostname;
use Net::Cassandra::Easy;
use Getopt::Long;
use POSIX;
use Time::HiRes qw( gettimeofday usleep );
use Term::ReadLine;
use Hash::Merge qw/merge/;
use Modern::Perl;
use Parse::RecDescent;

my %options =
 (
  verbose  => 0,
  quiet    => 0,
  server   => $ENV{CASSANDRA_SERVER},
  port     => $ENV{CASSANDRA_PORT},
  keyspace => $ENV{CASSANDRA_KEYSPACE},
 );

GetOptions (
            \%options,
            "debug|d",
            "quiet|q",
            "server=s",
            "port=i",
            "keyspace=s",
           );

$|=1;

my $debug = $Net::Cassandra::Easy::DEBUG = $options{debug};

use constant MAX_LONG => Bit::Vector->new_Bin(64, '0' . '1'x63);

use constant OFFSET_REGEX => qr/^[-+]\d+$/; # +1, +2, -1, -2, etc.

use constant TYPE_NUMERIC => 'numeric';
use constant TYPE_NONNUMERIC => 'other';

use constant FULL_KEYRANGE => [ range => { end_key => '', start_key => '' } ];

use constant COMPLETION_DONE => 'done';

use constant COMMAND_GET   => 'get';
use constant COMMAND_DEL   => 'del';
use constant COMMAND_INS   => 'ins';
use constant COMMAND_KEYS  => 'keys';
use constant COMMAND_DESC  => 'desc';
use constant COMMAND_ERROR => 'error';

use constant COMMANDS => [COMMAND_GET, COMMAND_DEL, COMMAND_INS, COMMAND_KEYS, COMMAND_DESC];

#die Dumper [matching_long_prefixes(shift @ARGV)]; # I haz test
#die Dumper [Net::Cassandra::Easy::unpack_decimal(Net::Cassandra::Easy::pack_decimal(shift @ARGV))]; # I haz test

my $quiet = $Net::Cassandra::Easy::QUIET = scalar @ARGV || $options{quiet}; # be quiet if this is non-interactive or if requested

my $c = Net::Cassandra::Easy->new(server => $options{server}, port => $options{port}, keyspace => $options{keyspace}, credentials => { none => 1 });;
$c->connect();

my %families;
my @families;

eval
{
    %families = %{$c->describe()};
    @families = sort keys %families;

    foreach my $family (@families)
    {
        next if $families{$family}->{super};
        say "Ignoring standard family $family (standard families are a TODO for a future version)" unless $quiet;
        delete $families{$family};
    }
    
    @families = sort keys %families;
};
    
if ($@)
{
    die "Startup error: " . Dumper($@);
}

my $grammar_text = <<'EOHIPPUS';
command: COMMAND_DESC | COMMAND_GET | COMMAND_DEL | COMMAND_INS | COMMAND_KEYS | <error>

completing_COMMAND_DESC: <rulevar: local $expecting = ''> | COMMAND_DESC { ['COMPLETION_DONE', $item[1] ] } | { $expecting }
COMMAND_DESC: <skip: ''> 'COMMAND_DESC'
{ $return = ['describe', \&::dump_hash]; 1; }

completing_COMMAND_GET: <rulevar: local $expecting = ''> | COMMAND_GET { $return = ['COMPLETION_DONE', $item[1] ] } | { $expecting }
COMMAND_GET:  <skip: ''> 'COMMAND_GET'
              { $expecting = ['family', {}] } ws family
              { $expecting = ['keys', { %{$item{family}} } ] } ws keys
              { $expecting = ['getparams', { %{$item{keys}}, %{$item{family}} } ] } ws getparams
{ $return = [ \&::internalPRD_get, \&::dump_hash, $item{family}, $item{keys}, $item{getparams} ]; }

completing_COMMAND_DEL: <rulevar: local $expecting = ''> | COMMAND_DEL { ['COMPLETION_DONE', $item[1] ] } | { $expecting }
COMMAND_DEL:  <skip: ''> 'COMMAND_DEL'
              { $expecting = ['family', {}] } ws family
              { $expecting = ['keys', { %{$item{family}} } ] } ws keys
              { $expecting = ['getparams', { %{$item{keys}}, %{$item{family}} } ] } ws getparams
{ $return = [ \&::internalPRD_delete, \&::dump_hash, $item{family}, $item{keys}, $item{getparams} ]; }

completing_COMMAND_INS: <rulevar: local $expecting = ''> | COMMAND_INS { ['COMPLETION_DONE', $item[1] ] } | { $expecting }
COMMAND_INS:  <skip: ''> 'COMMAND_INS' 
              { $expecting = ['family', {}] } ws family
              { $expecting = ['keys', { %{$item{family}} } ] } ws keys
              { $expecting = ['getparams', { %{$item{keys}}, %{$item{family}} } ] } ws getparams_nameonly
              { $expecting = ['insparams', { %{$item{keys}}, %{$item{family}}, @{$item{getparams_nameonly}} } ] } ws insparams
{ $return = [ \&::internalPRD_insert, \&::dump_hash, $item{family}, $item{keys}, $item{getparams_nameonly}, $item{insparams} ]; }

completing_COMMAND_KEYS: <rulevar: local $expecting = ''> | COMMAND_KEYS { ['COMPLETION_DONE', $item[1] ] } | { $expecting }
COMMAND_KEYS: <skip: ''> 'COMMAND_KEYS'
              { $expecting = ['family', {}] } ws family
{ $return = [ \&::internalPRD_keys, \&::dump_array, $item{family} ]; }

family: /\S+/ { $return = { family => $item[1] }; }

keys: key(s /,/) { $return = { keys => $item[1] }; }

key: /[^\s,]+/

getparams: getparam(s /,/)
getparams_nameonly: name(s /,/)

insparams: insparam(s /,/) { my $out = {}; $out = ::merge($out, $_) foreach @{$item[1]}; $return = { insert => $out }; }

insparam: inskey /=/ insvalue { $return = { $item{inskey} => $item{insvalue} }; }
inskey: /[^\s,=]+/
insvalue: /[^\s,=]+/

getparam: position | name

position: /[-+](\d+)/ { $return = { position => [$item[1]] }; }

name: /[^\s,]+/ { $return = { name => [$item[1]] }; }

ws: /\s+/

EOHIPPUS

$grammar_text =~ s/$_/eval $_/eg foreach qw/COMMAND_GET COMMAND_DEL COMMAND_INS COMMAND_KEYS COMMAND_DESC COMPLETION_DONE/;
print "Grammar: \n----\n$grammar_text\n----\n" if $debug;

my $grammar = new Parse::RecDescent($grammar_text);

#die Dumper $grammar->completing_get(shift); # I haz test

{
    my @rotate = qw,| / - \\,;
    sub next_rotate
    {
        my $rot = shift @rotate;
        push @rotate, $rot;
        return $rot;
    }
}

my @given = @ARGV;

my $term = new Term::ReadLine sprintf("Cassandra@%s:%s[%s]", @options{qw/server port keyspace/}) unless scalar @ARGV;
# $term->ornaments(0,1);

my $attribs = scalar @ARGV ? {} : $term->Attribs;

$attribs->{attempted_completion_function} = \&cass_PRDcompletion;

my $input;
while (defined ($input = shift @given || $term->readline('> ')) )
{
    run_command($c, $input);
    
    $term->addhistory($input) unless scalar @ARGV;

    exit if scalar @ARGV && 0 == scalar @given;
}

sub run_command
{
    my $c = shift @_;
    my $i = shift @_;

    my $parsed = $grammar->command($i);

    if (ref $parsed eq 'ARRAY' && defined $parsed->[0] && defined $parsed->[1])
    {
        my ($call, $print, @args) = @$parsed;
        my $params = {};

        foreach my $p (@args)
        {
            $p = [$p] if ref $p ne 'ARRAY';
            
            foreach my $spec (@$p)
            {
                if (ref $spec eq 'ARRAY')
                {
                    $params = merge($params, $_) foreach @$spec;
                }
                else
                {
                    $params = merge($params, $spec);
                }
            }
        }
        
        eval
        {
            print "Calling $call with args ", Dumper($params) if $debug;
            my $ret = ref $call eq 'CODE' ? $call->($c, $params) : $c->$call(%$params);
            print "Calling $call returned ", Dumper($ret) if $debug;
            print $print->($ret);
        };
        
        if ($@)
        {
            warn "Error: " . Dumper($@);
        }
    }
    else
    {
        warn "Input error: '$i' could not be parsed";
    }
}

sub cass_PRDcompletion
{
    my ($text, $line, $start, $end) = @_;

    my $given = substr($line, 0, $start);
    my $prefix = substr($line, $start, $end);

    my $completions = PRDcompletions($given, $prefix);

    $completions = [' '] unless $completions;
    
    $attribs->{completion_word} =  $completions;
    return $term->completion_matches($text, $attribs->{list_completion_function});
    
    # if (defined $parray)
    # {
    #   $attribs->{completion_word} =  [completions($command, $param, $text, $phash)];
    #   return $term->completion_matches($text, $attribs->{list_completion_function});
    # }
    
    # elsif (0)
    # {
    #   return $term->completion_matches($text, $attribs->{username_completion_function});
    # }
    # else # filename completion
    # {
    #   return (); # filename completion
    # }
}

sub PRDcompletions
{
    my $given = shift @_;
    my $prefix = shift @_;

    $prefix =~ s/\s*$//;

    if ($given =~ m/^\s*\S*$/)
    {
        return COMMANDS();
    }
#    my $at_end = $given =~ m/\s+$/;

    my $parsed;
    if ($given =~ m/^\s*(\S+)/)
    {
        my $method = "completing_$1";
        $parsed = $grammar->$method($given);
    }
    
    die "ERROR: could not parse input '$given'" unless defined $parsed;

    my ($expected, $ret, @rest) = @$parsed;
#    warn "prefix '$prefix', " . Dumper $parsed;
    return [ '' ] if $expected eq COMPLETION_DONE;

    my %structure = %$ret;
    
    given ($expected)
    {
        when ('family')
        {
            return [ sort keys %families ];
        }

        when ('keys')
        {
            my $family = $structure{family};
            return internalPRD_keys($c, { family => $family, prefix => $prefix });
        }

        when ('insparams')
        {
            return ["key1=value1,key2=value2"];
        }
        
        when ('getparams')
        {
            my $family = $structure{family};
            my $keys = $structure{keys};
            
            my $ranges = [];
            my $bitmasks = [];
            given(get_completion_type_for_family($family))
            {
                when (TYPE_NUMERIC)
                {
                    if ($prefix =~ m/([-+]?\d+)$/)
                    {
                        my $numeric_prefix = $1;
                        if ($numeric_prefix =~ m/^[-+]/) # we don't want a positional argument to match for completion so just return it as a valid completion
                        {
                            return [$numeric_prefix];
                        }
                        else
                        {
                            $ranges = [map { { count => Net::Cassandra::Easy::THRIFT_MAX, startlong => $_->[0], endlong => $_->[1] } } matching_long_prefixes($numeric_prefix)];
                        }
                    }
                }
                
                when (TYPE_NONNUMERIC)
                {
                    $bitmasks = [ $prefix ];
                }
            };
            
            my $positions = [];
            $positions = [-100] unless scalar @$ranges || scalar @$bitmasks;
            
            my $data = internalPRD_get($c, { family => $family, keys => $keys, ranges => $ranges, bitmasks => $bitmasks, position => $positions });
            return [sort keys %$data] if defined $data && ref $data eq 'HASH';
        }
    }
    
    return;
}

sub internalPRD_keys
{
    my $c      = shift @_;
    my $params = shift @_;

    my $families = [$params->{family}];
    my $prefix = $params->{prefix} || '';
    
    my @keys;
    eval
    {
        my $ret;

        $prefix =~ s/\s+//g;
#say "prefix: $prefix";

        if (length $prefix)
        {
            # TODO: figure out how to do a range query right, 0.7.0 trunk doesn't seem to filter correctly with OPP, probably because of hashes
            $ret = $c->keys($families, range => { end_key => '', start_key => $prefix });
        }
        else
        {
            $ret = $c->keys($families, @{FULL_KEYRANGE()});
        }

        foreach my $slice (@$ret)
        {
            push @keys, $_->key() foreach @$slice;
        }

        #printf "Got back %d keys not starting with $prefix\n", scalar grep { $_ !~ m/^$prefix/ } @keys;
    };

    if ($@)
    {
        warn "Error: " . Dumper($@);
    }

    return \@keys;
}

sub internalPRD_delete
{
    my $c      = shift @_;
    my $params = shift @_;

    my $family    = $params->{family};
    my $keys      = $params->{keys}     || [];
    my $names     = $params->{name}     || [];

    my $results = { };

    my $delete_spec = {
                       family => $family,
                      };


    $delete_spec->{deletions}->{family_byXYZ_specifier($family)} = $names;
    
    print "mutate() query: " . Dumper $delete_spec if $debug;

    eval
    {
        $results = $c->mutate($keys, %$delete_spec);
        say "Successful deletion" unless $quiet;
    };
    
    if ($@)
    {
        warn "Error: " . Dumper($@);
    }

    return $results;
}

sub internalPRD_insert
{
    my $c      = shift @_;
    my $params = shift @_;

    my $family    = $params->{family};
    my $keys      = $params->{keys}     || [];
    my $names     = $params->{name}     || [];
    my $insert    = $params->{insert}   || {};

    my $results = { };

    my $insert_spec = {
                       family => $family,
                      };

    $insert_spec->{insertions}->{packer($family, $_)} = $insert
     foreach @$names;
    
    print "mutate() query: " . Dumper $insert_spec if $debug;

    eval
    {
        $results = $c->mutate($keys, %$insert_spec);
        say "Successful insertion" unless $quiet;
    };
    
    if ($@)
    {
        warn "Error: " . Dumper($@);
    }

    return $results;
}

sub internalPRD_get
{
    my $c      = shift @_;
    my $params = shift @_;

    my $family    = $params->{family};
    my $keys      = $params->{keys}     || [];
    my $positions = $params->{position} || [];
    my $names     = $params->{name}     || [];
    my $ranges    = $params->{ranges}   || [];
    my $bitmasks  = $params->{bitmasks} || [];

    my @queries;
    foreach my $position (@$positions)
    {
        push @queries, [ family => $family, byoffset => { count => $position, start => '' } ]
    }

    foreach my $range (@$ranges)
    {
        push @queries, [ family => $family, byoffset => $range ]
    }

    push @queries, [ family => $family, family_byXYZ_specifier($family) => $names ] if @$names;

    push @queries, [ family => $family, bitmasks => $bitmasks, byoffset => { count => Net::Cassandra::Easy::THRIFT_MAX, start => '' } ] if @$bitmasks;
    
    my $results = {};
    print "get() queries: " . Dumper \@queries if $debug;
    eval
    {
        foreach my $query (@queries)
        {
            my %q = @$query;
            print next_rotate() unless $quiet;
            my $qret = $c->get($keys, @$query);
            print "\b \b" unless $quiet;

            my @return = map { values %$_ } values %$qret;
            my $ret = {};

            foreach my $r (@return)
            {
                foreach my $key (keys %$r)
                {
                    $ret->{unpacker($q{family}, $key)} = $r->{$key};
                }
            }

            $qret = $ret;
            
            printf "Query %s returned %s", Dumper($query), Dumper($qret) if $debug;
            $results = merge($results, $qret);
        }
    };

    if ($@)
    {
        warn "Error: " . Dumper($@);
    }

    return $results;
}

# find all the Long (8 byte) values that can match a string prefix, e.g. "123" -> (123,123) + (1230,1239) + (12300,12399) + ...
sub matching_long_prefixes
{
    my $prefix = shift @_;

    return [] if $prefix =~ OFFSET_REGEX;

    $prefix =~ s/\D+//g;
    $prefix ||= 0;

    my $pd = sub
    {
        my $ret;
        eval
        {
            $ret = Bit::Vector->new_Dec(64, shift)
        };

        return $ret || MAX_LONG;
    };

    my @ranges;

    my $cur = $prefix;
    my $curmax = $prefix;
    my $curmin = $prefix;

    while (MAX_LONG()->Compare($pd->($curmin)) > 0)
    {
        my $pdmax = $pd->($curmax);
        my $pdmin = $pd->($curmin);

        $pdmax = MAX_LONG if $pdmax->Sign() < 0;
        $pdmin = MAX_LONG if $pdmin->Sign() < 0;
        
        #warn "cur = $cur, max = $curmax, min = $curmin" . Dumper ([$pdmin->to_Dec(), $pdmax->to_Dec() ]);
        push @ranges, [ $pdmin->to_Dec(), $pdmax->to_Dec() ];

        $cur .= 'x';
        $curmax = $curmin = $cur;
        $curmax =~ s/x/9/g;
        $curmin =~ s/x/0/g;
    }

    return @ranges;
}

sub get_completion_type_for_family
{
    my $family = shift @_;

    return unless exists $families{$family};

    return TYPE_NUMERIC if $families{$family}->{cmp} eq 'Long';

    return TYPE_NONNUMERIC;
}

sub family_byXYZ_specifier
{
    given(get_completion_type_for_family(shift))
    {
        when (TYPE_NUMERIC)
        {
            return 'bylong';
        }
        
        when (TYPE_NONNUMERIC)
        {
            return 'byname';
        }

        default
        {
            return "byname";
        }
    }
    
}

sub family_packerunpacker
{
    my $family = shift @_;

    given(get_completion_type_for_family($family))
    {
        when (TYPE_NUMERIC)
        {
            return [
                    sub { return Net::Cassandra::Easy::pack_decimal(shift) },
                    sub { return Net::Cassandra::Easy::unpack_decimal(shift) },
                   ]
        }
        
        when (TYPE_NONNUMERIC)
        {
            return [ sub { shift }, sub { shift } ];
        }
    }
}

sub packer
{
    my $family = shift @_;
    my $v      = shift @_;
    return family_packerunpacker($family)->[0]->($v);
}

sub unpacker
{
    my $family = shift @_;
    my $v      = shift @_;
    return family_packerunpacker($family)->[1]->($v);
}

sub dump_hash
{
    my $h = shift @_;
    my $phash = shift @_;
    my $prefix = shift @_ || '';

    my $family = '';
    $family = "$phash->{family}:" if defined $phash && exists $phash->{family};

    foreach my $key (sort keys %$h)
    {
        if (ref $h->{$key} eq 'HASH')
        {
            dump_hash($h->{$key}, $phash, $key);
        }
        else
        {
            printf "%s%s.%s=%s\n", $family, $prefix, $key, $h->{$key};
        }
    }
}

sub dump_array
{
    my $a = shift @_;
    my $phash = shift @_;

    my $family = '';
    $family = $phash->{family} if defined $phash && exists $phash->{family};

    $family .= ':' if $family;

    foreach my $key (sort @$a)
    {
        say $family, $key;
    }
}
