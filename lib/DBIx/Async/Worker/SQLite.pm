package DBIx::Async::Worker::SQLite;
use strict;
use warnings;
use DBI;
use DBD::SQLite;
use Try::Tiny;

use constant DEBUG => 0;

sub new {
	my $class = shift;
	bless { @_ }, $class
}

sub ret_ch { shift->{ret_ch} }
sub sth_ch { shift->{sth_ch} }
sub parent { shift->{parent} }

sub run {
	my $self = shift;
	my $dbh = DBI->connect(
		$self->parent->dsn,
		$self->parent->user,
		$self->parent->pass,
		$self->parent->options
	);

	# This doesn't really serve any valid purpose with
	# sqlite running in a separate process, but it
	# can make writes more predictable.
	warn "Enable journal mode...\n" if DEBUG;
	$dbh->do(q{PRAGMA journal_mode=WAL});
	warn "Disable autocheckpoint...\n" if DEBUG;
	$dbh->do(q{PRAGMA wal_autocheckpoint=0});
	warn "Switch to sync=NORMAL...\n" if DEBUG;
	$dbh->do(q{PRAGMA synchronous=NORMAL});

	my %sth;
	# TODO Make these into methods
	my %handler = (
		do => sub {
			my $op = shift;
			$dbh->do($op->{sql});	
			return { status => 'ok' };
		},
		begin_work => sub {
			my $op = shift;
			$dbh->begin_work;
			return { status => 'ok' };
		},
		commit => sub {
			my $op = shift;
			$dbh->commit;
			return { status => 'ok' };
		},
		rollback => sub {
			my $op = shift;
			$dbh->rollback;
			return { status => 'ok' };
		},
		prepare => sub {
			my $op = shift;
			my $sth = $dbh->prepare($op->{sql});
			$sth{$sth} = $sth;
			return { status => 'ok', id => "$sth" };
		},
		execute => sub {
			my $op = shift;
			my $sth = $sth{$op->{id}} or return {
				status => 'fail',
				message => 'invalid ID'
			};
			$sth->execute;
			return { status => 'ok', id => "$sth" };
		},
		fetchrow_hashref => sub {
			my $op = shift;
			my $sth = $sth{$op->{id}} or return {
				status => 'fail',
				message => 'invalid ID'
			};
			my $data = $sth->fetchrow_hashref;
			return { status => 'ok', id => "$sth", data => $data };
		},
	);

	if(1) {
		$dbh->sqlite_commit_hook(sub {
			warn "Manual checkpoint...\n" if DEBUG;
			my $sth = $dbh->prepare(q{PRAGMA wal_checkpoint(FULL)});
			$sth->execute;
			while(my $row = $sth->fetchrow_arrayref) {
				warn "Checkpoint result: @$row\n" if DEBUG;
			}
			warn "Done\n" if DEBUG;
			0
		});
	}
	while(my $data = $self->sth_ch->recv) {
		if(exists $handler{$data->{op}}) {
			try {
				$self->ret_ch->send($handler{$data->{op}}->($data));
			} catch {
				$self->ret_ch->send({ status => 'fail', message => $_ });
			};
		} else {
			$self->ret_ch->send({ status => 'fail', message => 'unknown operation' });
		}
	}
	warn "End of sqlite subprocess\n" if DEBUG;
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2012-2013. Licensed under the same terms as Perl itself.

