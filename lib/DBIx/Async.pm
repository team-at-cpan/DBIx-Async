package DBIx::Async;
# ABSTRACT: use DBI with IO::Async
use strict;
use warnings;
use parent qw(IO::Async::Notifier);

our $VERSION = '0.001';

=head1 NAME

DBIx::Async - use L<DBI> with L<IO::Async>

=head1 SYNOPSIS

 use feature qw(say);
 use IO::Async::Loop;
 use DBIx::Async;
 my $loop = IO::Async::Loop->new;
 say 'Connecting to db';
 $loop->add(my $dbh = DBIx::Async->new(
   'dbi:SQLite:dbname=test.sqlite3',
   '',
   '', {
     AutoCommit => 1,
     RaiseError => 1,
   }
 ));
 $dbh->do(q{CREATE TABLE tmp(id integer primary key autoincrement, content text)})
 # ... put some values in it
 ->and_then(sub { $dbh->do(q{INSERT INTO tmp(content) VALUES ('some text'), ('other text') , ('more data')}) })
 # ... and then read them back
 ->and_then(sub {
   # obviously you'd never really use * in a query like this...
   my $sth = $dbh->prepare(q{select * from tmp});
   $sth->execute;
   # the while($row = fetchrow_hashref) construct isn't a good fit
   # but we attempt to provide something reasonably close using the
   # ->iterate helper
   $sth->iterate(
     fetchrow_hashref => sub {
       my $row = shift;
       say "Row: " . join(',', %$row);
     }
   );
 })->on_done(sub {
   say "Query complete";
   $loop->stop;
 })->on_fail(sub { warn "Failure: @_\n" });
 $loop->run;

=head1 DESCRIPTION

Wrapper for L<DBI>, for running queries much slower than usual but without blocking.

=head2 PERFORMANCE

Greatly lacking. See C<examples/benchmark.pl>, in one sample run the results looked
like this:

             s/iter DBIx::Async DBD::SQLite
 DBIx::Async   5.49          --        -95%
 DBD::SQLite  0.280       1861%          --

=head1 METHODS

Where possible, L<DBI> method signatures are used.

=cut

use IO::Async::Channel;
use IO::Async::Routine;
use Future;
use Module::Load qw();
use Try::Tiny;
use DBI;

use DBIx::Async::Handle;

use constant DEBUG => 0;

=head2 connect

Attempts to connect to the given DSN.

Takes the following options:

=over 4

=item * $dsn - the data source name, should be something like 'dbi:SQLite:dbname=:memory:'

=item * $user - username to connect as

=item * $pass - password to connect with

=item * $opt - any options

=back

Options consist of:

=over 4

=item * RaiseError - set this to 1

=item * AutoCommit - whether to run in AutoCommit mode by default, probably works better
if this is set to 1 as well

=back

Returns $self.

=cut

sub connect {
	my $class = shift;
	my ($dsn, $user, $pass, $opt) = @_;
	my $self = bless {
		options => $opt,
		pass => $pass,
		user => $user,
		dsn => $dsn,
	}, $class;
	$self
}

=head2 dsn

Returns the DSN used in the L</connect> request.

=cut

sub dsn { shift->{dsn} }

=head2 user

Returns the username used in the L</connect> request.

=cut

sub user { shift->{user} }

=head2 pass

Returns the password used in the L</connect> request.

=cut

sub pass { shift->{pass} }

=head2 options

Returns any options that were set in the L</connect> request.

=cut

sub options { shift->{options} }

=head2 do

Returns $self.

=cut

sub do {
	my $self = shift;
	my $sql = shift;
	$self->queue({ op => 'do', sql => $sql });
}

=head2 begin_work

Returns $self.

=cut

sub begin_work {
	my $self = shift;
	my $sql = shift;
	$self->queue({ op => 'begin_work' });
}

=head2 commit

Returns $self.

=cut

sub commit {
	my $self = shift;
	my $sql = shift;
	$self->queue({ op => 'commit' });
}

=head2 prepare

Returns $self.

=cut

sub prepare {
	my $self = shift;
	my $sql = shift;
	DBIx::Async::Handle->new(
		dbh => $self,
		prepare => $self->queue({ op => 'prepare', sql => $sql }),
	);
}

=head2 queue

Returns $self.

=cut

sub queue {
	my $self = shift;
	my ($req, $code) = @_;
	my $f = Future->new;
	if(DEBUG) {
		require Data::Dumper;
		warn "Sending req " . Data::Dumper::Dumper($req);
	}
	$self->sth_ch->send($req);
	$self->ret_ch->recv(
		on_recv => sub {
			my ( $ch, $rslt ) = @_;
			if($rslt->{status} eq 'ok') {
				$f->done($rslt);
			} else {
				$f->fail($rslt->{message});
			}
		}
	);
	$f
}

=head1 INTERNAL METHODS

These are unlikely to be of much use in application code.

=cut

=head2 worker_class_from_dsn

Returns $self.

=cut

sub worker_class_from_dsn {
	my $self = shift;
	my $dsn = shift;
	my ($dbd) = $dsn =~ /^dbi:([^:]+)(?::|$)/;
	die "Invalid DBD class: $dbd" unless $dbd =~ /^[a-zA-Z0-9]+$/;
	my $loaded;
	my $class;
	for my $subclass ($dbd, 'Default') {
		last if $loaded;
		$class = 'DBIx::Async::Worker::' . $subclass;
		try {
			Module::Load::load($class);
			$loaded = 1
		} catch {
			warn "class load: $_\n" if DEBUG;
		};
	}
	die "Could not find suitable class for $dbd" unless $loaded;
	$class;
}

=head2 sth_ch

Returns $self.

=cut

sub sth_ch { shift->{sth_ch} }

=head2 ret_ch

Returns $self.

=cut

sub ret_ch { shift->{ret_ch} }

=head2 _add_to_loop

Returns $self.

=cut

sub _add_to_loop {
	my $self = shift;
	my ($loop) = @_;
	my $worker = $self->worker_class_from_dsn($self->dsn)->new(
		parent => $self,
		sth_ch => ($self->{sth_ch} = IO::Async::Channel->new),
		ret_ch => ($self->{ret_ch} = IO::Async::Channel->new),
	);
	my $routine = IO::Async::Routine->new(
		channels_in  => [ $self->{sth_ch} ],
		channels_out => [ $self->{ret_ch} ],
		code => sub { $worker->run },
		on_finish => sub {
			print "The routine aborted early - $_[-1]\n";
			$self->loop->stop;
		},
	);
	$loop->add($routine);
}

=head2 _remove_from_loop

Returns $self.

=cut

sub _remove_from_loop {
	my $self = shift;
	my ($loop) = @_;
	warn "Removed from loop\n";
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2012-2013. Licensed under the same terms as Perl itself.

