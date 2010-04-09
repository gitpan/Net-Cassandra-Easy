#!perl

package Net::Cassandra::Easy;

use Moose;
use warnings;
use strict;

use constant 1.01;                      # don't omit this! needed for resolving the AccessLevel constants

use Data::Dumper;
use Bit::Vector;

use Class::Accessor;

use Time::HiRes qw( gettimeofday );

use Net::GenCassandra::Cassandra;
use Net::GenCassandra::Types;
use Net::GenCassandra::Constants;
use Net::GenThrift::Thrift::Socket;
use Net::GenThrift::Thrift::BinaryProtocol;
use Net::GenThrift::Thrift::FramedTransport;
use Net::GenThrift::Thrift::BufferedTransport;

our $VERSION = "0.10";

our $DEBUG = 0;
our $QUIET = 0;

use constant THRIFT_MAX => 100;

# plain options, required for construction
has server      => ( is => 'ro', isa => 'Str', required => 1 );
has keyspace    => ( is => 'ro', isa => 'Str', required => 1 );
has credentials => ( is => 'ro', isa => 'HashRef', required => 0 );

# plain options with defaults
has port         => ( is => 'ro', isa => 'Int',     default => 9160 );
has recv_timeout => ( is => 'ro', isa => 'Int',     default => 5000 );
has send_timeout => ( is => 'ro', isa => 'Int',     default => 1000 );
has recv_buffer  => ( is => 'ro', isa => 'Int',     default => 1024 );
has send_buffer  => ( is => 'ro', isa => 'Int',     default => 1024 );
has max_results  => ( is => 'ro', isa => 'Int',     default => THRIFT_MAX );

has timestamp    => (
                     is => 'ro', isa => 'CodeRef',
                     default => sub
                     {
                         sub
                         {
                             return sprintf "%d%0.6d", gettimeofday();
                         }
                     }
                    );

# read and write consistency can be changed on the fly
has read_consistency  => ( is => 'rw', isa => 'Int', default => Net::GenCassandra::ConsistencyLevel::ONE );
has write_consistency => ( is => 'rw', isa => 'Int', default => Net::GenCassandra::ConsistencyLevel::ONE );

# internals
has socket    => (is => 'rw', isa => 'Net::GenThrift::Thrift::Socket');
has protocol  => (is => 'rw', isa => 'Net::GenThrift::Thrift::BinaryProtocol');
has client    => (is => 'rw', isa => 'Net::GenCassandra::CassandraClient');
has transport => (is => 'rw', isa => 'Net::GenThrift::Thrift::BufferedTransport');
has opened    => (is => 'rw', isa => 'Bool');

# use constant GRAMMAR_SPECIAL => 'special';
# use constant GRAMMAR_EXACT   => 'exact';
# use constant GRAMMAR_ALL     => 'ALL';

our $last_predicate = Net::GenCassandra::SlicePredicate->new({
                                                              slice_range => Net::GenCassandra::SliceRange->new({start=> '' , finish=> '', reversed => 1, count => 1}),
                                                             });

our $first_predicate = Net::GenCassandra::SlicePredicate->new({
                                                               slice_range => Net::GenCassandra::SliceRange->new({start=> '' , finish=> '', reversed => 1, count => 1}),
                                                              });

our $all_predicate = Net::GenCassandra::SlicePredicate->new({
                                                             slice_range => Net::GenCassandra::SliceRange->new({start=> '' , finish=> ''}),
                                                            });

sub validate_array
{
    my $data = shift @_;
    my $name = shift @_;
    my $info = shift @_ || '';

    die "Sorry but you didn't specify anything for the $name" unless $data;
    die "Sorry but you didn't specify the $name as an array" unless ref $data eq 'ARRAY';
    die "Sorry but you didn't specify anything in the $name array in $info" unless @$data;
}

sub validate_hash
{
    my $data = shift @_;
    my $name = shift @_;
    my $info = shift @_;

    die "Sorry but you didn't specify anything for the $name" unless $data;
    die "Sorry but you didn't specify the $name as a hash" unless ref $data eq 'HASH';
    die "Sorry but you didn't specify anything in the $name hash in $info" unless scalar keys %$data;
}

sub validate_insertion_hash
{
    my $data = shift @_;
    my $name = shift @_;
    my $info = shift @_;

    foreach my $key (sort keys %$data)
    {
        die "Sorry but $name data key $key points to an undefined value in $info" unless defined $data->{$key};
    }
}

sub validate_family
{
    my $family = shift @_;
    my $info = shift @_;

    die "Sorry but you have to specify the family in $info" unless $family;
}

sub validate_predicate
{
    my $spec = shift @_;
    my $info = shift @_;

    my $offsets  = $spec->{byoffset};
    my $named    = $spec->{byname};
    my $longs    = $spec->{bylong};
    my $bitmasks = $spec->{bitmasks};
    my $standard = $spec->{standard} || 0;

    my $bycount = !!$offsets + !!$named + !!$longs;

    my @bitmasks = (bitmasks => $bitmasks) if $bitmasks;

    if (!$standard)
    {
        die "Sorry but you have to specify EXACTLY ONE of a 'byoffset' or a 'byname' or a 'bylong' key for supercolumns in $info" if $bycount != 1;
    }
    elsif ($named) # specific column deletions
    {
        return Net::GenCassandra::SlicePredicate->new({
                                                       column_names => $named,
                                                      });
    }
    else # we don't care about all the other options, just get the columns in this family
    {
        return $all_predicate;
    }

    if ($offsets)
    {
        die "Sorry but 'byoffset' has to be a hash in $info" unless ref $offsets eq 'HASH';
        die "Sorry but 'byoffset' has to have a 'count' key in $info" unless exists $offsets->{count};
        die "Sorry but 'byoffset' can't have both a 'start' and a 'startlong' key in $info" if exists $offsets->{start} && exists $offsets->{startlong};
        die "Sorry but 'byoffset' can't have both a 'finish' and a 'finishlong' key in $info" if exists $offsets->{finish} && exists $offsets->{finishlong};

        my $start = '';
        my $finish = '';

        $start = $offsets->{start} if exists $offsets->{start};
        $finish = $offsets->{finish} if exists $offsets->{finish};

        $start = pack_decimal($offsets->{startlong}) if exists $offsets->{startlong};
        $finish = pack_decimal($offsets->{finishlong}) if exists $offsets->{finishlong};

        return Net::GenCassandra::SlicePredicate->new({
                                                       slice_range => Net::GenCassandra::SliceRange->new({
                                                                                                          @bitmasks,
                                                                                                          start    => $start,
                                                                                                          finish   => $finish,
                                                                                                          reversed => 0+ ($offsets->{count} < 0),
                                                                                                          count    => abs($offsets->{count}),
                                                                                                         }),
                                                      });
    }

    if ($longs || $named)
    {
        my @columns;

        if ($longs)
        {
            die "Sorry but 'bylong' has to be an array in $info" unless (ref $longs eq 'ARRAY');
            @columns = map { pack_decimal($_) } @$longs;
        }

        if ($named)
        {
            die "Sorry but 'byname' has to be an array in $info" unless (ref $named eq 'ARRAY');
            @columns = @$named;
        }

        return Net::GenCassandra::SlicePredicate->new({
                                                       column_names => \@columns,
                                                      });
    }

    # if we get here, we don't know what we are doing
    die "Sorry but we couldn't handle $info";
}

sub validate_mutations
{
    my $self   = shift @_;
    my $spec   = shift @_;
    my $rows   = shift @_;
    my $family = shift @_;
    my $info   = shift @_;

    my $d = $spec->{deletions};
    my $i = $spec->{insertions};

    die "Sorry but you have to specify either some insertions or some deletions in $info" unless ($d || $i);

    my $out = {};

    if ($d)
    {
        validate_hash($d, 'deletions', $info);

        my $predicate = validate_predicate($d, $info);
        my $standard = $d->{standard} || 0;

        my $cols = $predicate->column_names();

        $cols = ['unused'] if $standard;

        if ($cols)
        {
            foreach my $row (@$rows)
            {
                my @mutes = map {
                    Net::GenCassandra::Mutation->new({
                                                      deletion => Net::GenCassandra::Deletion->new({
                                                                                                    $standard ? (predicate => $predicate) : (super_column => $_),
                                                                                                    timestamp => $self->timestamp()->(),
                                                                                                   }),
                                                     }),
                                                 } @$cols;

                push @{$out->{$row}->{$family}}, @mutes;
            }
        }
        else
        {
            die "Sorry, since Deletions don't support SliceRanges yet, a predicate based on them (using byoffset) won't work in $info";
        }
    }

    if ($i)
    {
        validate_hash($i, 'insertions (as hash)', $info);

        my $super_mode = ref ((values %$i)[0]) eq 'HASH';

        foreach my $row (@$rows)
        {
            if ($super_mode)
            {
                foreach my $sc_name (keys %$i)
                {
                    my $sc_spec = $i->{$sc_name};
                    validate_hash($sc_spec, 'insert.supercolumn parameter', $info);
                    validate_insertion_hash($sc_spec, 'insert.supercolumn parameter', $info);

                    my @cols = map
                    {
                        Net::GenCassandra::Column->new({
                                                        name=> $_,
                                                        value=> $sc_spec->{$_},
                                                        timestamp => $self->timestamp()->(),
                                                       }),
                                                   } keys %$sc_spec;

                    my $sc = Net::GenCassandra::ColumnOrSuperColumn->new({
                                                                          super_column => Net::GenCassandra::SuperColumn->new({
                                                                                                                               name => $sc_name,
                                                                                                                               columns => \@cols,
                                                                                                                              }),
                                                                         });

                    push @{$out->{$row}->{$family}}, Net::GenCassandra::Mutation->new({
                                                                                       column_or_supercolumn => $sc,
                                                                                      });
                }
            }
            else
            {
                my @mutes = map
                {
                    Net::GenCassandra::Mutation->new({
                                                      column_or_supercolumn => 
                                                      Net::GenCassandra::ColumnOrSuperColumn->new({
                                                                                                   column => Net::GenCassandra::Column->new({
                                                                                                                                             name=> $_,
                                                                                                                                             value=> $i->{$_},
                                                                                                                                             timestamp => $self->timestamp()->(),
                                                                                                                                            }),
                                                                                                  }),
                                                     });
                } keys %$i;

                push @{$out->{$row}->{$family}}, @mutes;
            }
        }
    }

    return $out;
}

# with batch_mutate we can do deletion and insertion
sub mutate
{
    my $self = shift @_;

    die "How am I supposed to talk to the server if you haven't connect()ed?" unless $self->opened();

    die "Sorry but there were no parameters, you need to ask me for something!" unless scalar @_;

    my $rows = shift @_;
    my %spec = @_;

    my $fallback_rows = $rows || [];
    $fallback_rows = [] unless ref $rows eq 'ARRAY';

    my $info = "mutate() request with rows [@$fallback_rows] and spec " . Dumper(\%spec) . "\n";

    validate_array($rows, 'rows', $info);

    my $family  = $spec{family};

    validate_family($family, $info);

    my $mutation_map = $self->validate_mutations(\%spec, $rows, $family, $info);

    if ($DEBUG)
    {
        my $mutation_dump = Dumper($mutation_map);
        print "Constructed mutation $mutation_dump from $info";
    }

    # void batch_mutate(1:required string keyspace,
    #                   2:required map<string, map<string, list<Mutation>>> mutation_map,
    #                   3:required ConsistencyLevel consistency_level=ZERO)
    #      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),
    print "Running batch_mutate in $info" if $DEBUG;

    my $result = $self->client()->batch_mutate(
                                               $self->keyspace(),
                                               $mutation_map,
                                               $self->read_consistency()
                                              );
    return $result;
}

# describe the keyspace
sub describe
{
    my $self = shift @_;

    die "How am I supposed to talk to the server if you haven't connect()ed?" unless $self->opened();

    my $d = $self->client()->describe_keyspace($self->keyspace());

    # print "Raw describe_keyspace() result is " . Dumper($d) if $DEBUG;

    my $ret = {};

    foreach my $key (keys %$d)
    {
        $ret->{$key} = {
                        super => $d->{$key}->{Type} eq 'Super',
                        cmp => parse_type($d->{$key}->{CompareWith}),
                        subcmp => parse_type($d->{$key}->{CompareSubcolumnsWith}),
                        sort => parse_type($d->{$key}->{Desc}),
                       };
    }

    # print "Interpreted describe_keyspace() result is " . Dumper($ret) if $DEBUG;

    return $ret;
}

sub parse_type
{
    my $type = shift @_;

    return '' unless defined $type;

    $type =~ s/.*org.apache.cassandra.db.marshal.(\w+).*/$1/s;
    $type =~ s/Type$//;

    return $type;
}

sub keys
{
    my $self = shift @_;

    die "How am I supposed to talk to the server if you haven't connect()ed?" unless $self->opened();

    my $families = shift @_;
    my %spec = @_;

    my $fallback_families = $families || [];
    $fallback_families = [] unless ref $families eq 'ARRAY';

    my $info = "keys() request with families [@$fallback_families] and spec " . Dumper(\%spec) . "\n";

    validate_array($families, 'families', $info);

    # list<KeySlice> get_range_slices(1:required string keyspace, 
    #                                 2:required ColumnParent column_parent, 
    #                                 3:required SlicePredicate predicate,
    #                                 4:required KeyRange range,
    #                                 5:required ConsistencyLevel consistency_level=ONE)
    #                throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

    my @ret;

    foreach my $family (@$families)
    {
        my $parent = Net::GenCassandra::ColumnParent->new({
                                                           column_family => $family,
                                                          });

        my $key_range = validate_keyrange(\%spec);

        if ($DEBUG)
        {
            printf "Constructed key range %s from spec %s", Dumper($key_range), Dumper(\%spec);;
        }

        my $r = $self->client()->get_range_slices($self->keyspace(),
                                                  $parent,
                                                  $first_predicate,
                                                  $key_range,
                                                  $self->read_consistency(),
                                                 );
        push @ret, $r;
    }

    return \@ret;
}

sub validate_keyrange
{
    my $spec   = shift @_;
    my $info   = shift @_;

    my $r = $spec->{range};

    die "Sorry but the range parameter is needed." unless $r;

    my $init = {};

    validate_hash($r, 'keyrange.offsets', $info);

    foreach my $k (qw/start_key end_key start_token end_token count/)
    {
        next unless exists $r->{$k};
        $init->{$k} = $r->{$k};
    }

    return Net::GenCassandra::KeyRange->new($init);
}

# with multiget_slice we can emulate all the others
sub get
{
    my $self = shift @_;

    die "How am I supposed to talk to the server if you haven't connect()ed?" unless $self->opened();

    die "Sorry but there were no parameters, you need to ask me for something!" unless scalar @_;

    my $rows = shift @_;
    my %spec = @_;

    my $fallback_rows = $rows || [];
    $fallback_rows = [] unless ref $rows eq 'ARRAY';

    my $info = "get() request with rows [@$fallback_rows] and spec " . Dumper(\%spec) . "\n";

    validate_array($rows, $info);

    my $family  = $spec{family};

    validate_family($family, $info);

    my $predicate = validate_predicate(\%spec, $info);

    if ($DEBUG)
    {
        my $predicate_dump = Dumper($predicate);
        print "Constructed predicate $predicate_dump from $info";
    }

    my $parent = Net::GenCassandra::ColumnParent->new({
                                                       column_family => $family,
                                                      });

    # map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
    print "Running multiget_slice in $info" if $DEBUG;
    my $result = $self->client()->multiget_slice(
                                                 $self->keyspace(),
                                                 $rows,
                                                 $parent,
                                                 $predicate,
                                                 $self->read_consistency()
                                                );

    #print "multiget_slice result = " . Dumper($result) if $DEBUG;

    return simplify_result($result, $family);

}

# map the results (ColumnOrSuperColumn objects) back into a hash
sub simplify_result
{
    my $result = shift @_;
    my $family = shift @_;

    if (ref $result eq 'HASH')
    {
        foreach my $key (CORE::keys(%$result))
        {
            my $r = {};

            foreach my $col (@{$result->{$key}})
            {
                if (ref $col eq 'Net::GenCassandra::ColumnOrSuperColumn')
                {
                    if (defined $col->column()) # is this a column?
                    {
                        $r->{$col->column()->name()} = $col->column()->value();
                    }
                    else # this is a supercolumn, map all its columns as a (column_name, column_value) hash ref to the supercolumn name as a key
                    {
                        $r->{$col->super_column()->name()} = {
                                                              map
                                                              {
                                                                  $_->name() => $_->value()
                                                              } @{$col->super_column->columns()}
                                                             };
                    }
                }
                else # fallback, just insert the value as a key and it will look odd enough to investigate
                {
                    $r->{$_} = 1;
                }
            }

            $result->{$key} = { $family => $r };
        }
    }

    return $result;
}

# from http://www.perlmonks.org/?node_id=163123
sub pack_decimal
{
    return pack_bv(Bit::Vector->new_Dec(64, "".shift));
}

sub pack_bv
{
    my $vec = shift;
    return pack 'NN', $vec->Chunk_Read(32, 32), $vec->Chunk_Read(32, 0);
}

sub unpack_decimal
{
    my @p = unpack('NN', shift);
    my $vec = Bit::Vector->new(64);
    $vec->Chunk_Store(32,32,$p[0]);
    $vec->Chunk_Store(32,0,$p[1]);
    return $vec->to_Dec();
}

sub make_remove_path
{
    my $family = shift @_;
    my $supers = shift @_ || [undef];
    my $cols   = shift @_ || [undef];

    my @ret;

    foreach my $s (@$supers)
    {
        foreach my $c (@$cols)
        {
            push @ret, Net::GenCassandra::ColumnPath->new({
                                                           column_family => $family,
                                                           super_column => $s,
                                                           column => $c,
                                                          });

        }
    }

    return \@ret;
}

sub connect
{
    my $self = shift @_;
    eval
    {
        $self->socket(Net::GenThrift::Thrift::Socket->new($self->server(), $self->port()));
        $self->socket()->setSendTimeout($self->send_timeout());
        $self->socket()->setRecvTimeout($self->recv_timeout());
        $self->transport(Net::GenThrift::Thrift::BufferedTransport->new($self->socket(), $self->send_buffer(), $self->recv_buffer()));
        $self->protocol(Net::GenThrift::Thrift::BinaryProtocol->new($self->transport()));
        $self->client(Net::GenCassandra::CassandraClient->new($self->protocol()));

        $self->transport()->open();
        $self->opened(1);

        if ($self->credentials())
        {
            my $level = $self->client()->login($self->keyspace(), new Net::GenCassandra::AuthenticationRequest({credentials => $self->credentials()}));

            # all this because Thrift doesn't record constants it will declare
            my $name = 'unknown_access_level';
            foreach my $constant (grep m/^Net::GenCassandra::AccessLevel::/, CORE::keys(%constant::declared))
            {
                $name = $constant if $level == eval $constant;
            }

            $name =~ s/.*:://;
            print "Authorized access level is $level ($name)\n" unless $QUIET;
        }
    };

    handle_errors();
}

sub handle_errors
{
    if ($@)
    {
        if ($@->can('why'))
        {
            die $@->why;
        }
        else
        {
            die Dumper($@) if $@;
        }
    }
}

1;

__END__

=pod

=head1 NAME

Net::Cassandra::Easy - Perlish interface to the Cassandra database

=head1 SYNOPSIS

  use Net::Cassandra::Easy;

  $Net::Cassandra::Easy::DEBUG = 1; # to see the Thrift structures and other fun stuff

  # this will login() with no credentials so only AllowAllAuthenticator will work
  my $c = Net::Cassandra::Easy->new(server => 'myserver', port => 'any port but default is 9160', keyspace => 'mykeyspace', credentials => { none => 1 });
  $c->connect();

  my $key = 'processes';

  my $result;

  # see test.pl for more examples, including insertions and deletions (with the mutate() call)

  $result = $c->get([$key], family => 'myfamily', byoffset => { count => -1 }); # last supercolumn, e.g. "latest" in LongType with timestamps

  $result = $c->get([$key], family => 'myfamily', byoffset => { count => 1 }); # first supercolumn, e.g. "earliest" in LongType with timestamps

  $result = $c->get([$key], family => 'myfamily', byoffset => { start => 'abcdefgh', count => 1 }); # first supercolumn after the 8 bytes 'abcdefgh'

  $result = $c->get([$key], family => 'myfamily', byoffset => { startlong => '100', finishlong => '101', count => 1 }); # first supercolumn after the Long (8 bytes) 100 and before the 8-byte Long 101, both Longs in a string so they will work in 32-bit Perl

  $result = $c->get([$key], family => 'myfamily', byname => [qw/one two/ ]); # get two supercolumns by name

  $result = $c->get([$key], family => 'myfamily', bylong => [0, 1, '10231024'); # get three supercolumns by name as an 8-byte Long (note the last one is a quoted string so it will work in 32-bit Perl)

  $result = $c->mutate([$key], family => 'myfamily', insertions => { 'hello!!!' => { testing => 123 } } ]) # insert SuperColumn named 'hello!!!' with one Column

  $result = $c->mutate([$key], family => 'myfamily', insertions => { Net::Cassandra::Easy::pack_decimal(0) => { testing => 123 } } ]) # insert SuperColumn named 0 (as a long with 8 bytes) with one Column

  $result = $c->mutate([$key], family => 'myfamily', deletions => { byname => ['hello!!!'] } ]) # delete SuperColumn named 'hello!!!'

  $result = $c->mutate([$key], family => 'myfamily', deletions => { bylong => [123] } ]) # delete SuperColumn named 123

  $result = $c->mutate([$key], family => 'Standard1', deletions => { standard => 1, byname => ['one', 'two'] } ]) # delete columns from a row in a non-super column family

  $result = $c->mutate([$key], family => 'Standard1', insertions => { testing => 123 } ]) # insert Columns into a non-super column family

  $result = $c->describe(, # describe the keyspace families

  $result = $c->keys(['myfamily'], range => { start_key => 'z', end_key => 'a', count => 100} ]) # list keys from 'a' to 'z', max 100

  $result = $c->keys(['myfamily'], range => { start_token => 0, end_token => 1, count => 100} ]) # list keys from token 0 to token 1, max 100

  print Dumper $result; # enjoy

=head1 DESCRIPTION

Net::Cassandra::Easy aims to simplify the basic interactions with the
Cassandra database.

Under the covers it translates every request to the Thrift API.  It
will stay current with that API as much as possible; I participate in
the Cassandra project and watch the mailing lists so any changes
should be in Net::Cassandra::Easy quickly.

How is it better than Net::Cassandra?

Net::Cassandra::Easy tries to stay away from Thrift.  Thus it's easier
to use in my opinion, and when and if Cassandra starts using another
API, e.g. Avro, Net::Cassandra::Easy will not change much.

How do the timestamps work?

Net::Cassandra::Easy uses microsecond-resolution timestamps (whatever
Time::HiRes gives us, basically).  You can override the timestamps
with the C<timestamp> initialization parameter, which takes a
subroutine reference.

=head2 EXPORT

Nothing, it's all methods on the client object.

=head1 AUTHOR

Teodor Zlatanov <tzz@lifelogs.com>

=head1 THANKS

Mike Gallamore <mike.e.gallamore@googlemail.com>

=head1 SEE ALSO

perl(1).

perldoc Net::Cassandra

=cut
