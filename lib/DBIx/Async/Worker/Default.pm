package DBIx::Async::Worker::Default;
use strict;
use warnings;
use DBI;
use Try::Tiny;

use constant DEBUG => 0;

=head2 new

Returns $self.

=cut

sub new {
	my $class = shift;
	bless { @_ }, $class
}

=head2 ret_ch

Returns $self.

=cut

sub ret_ch { shift->{ret_ch} }

=head2 sth_ch

Returns $self.

=cut

sub sth_ch { shift->{sth_ch} }

=head2 parent

Returns $self.

=cut

sub parent { shift->{parent} }

=head2 run

Returns $self.

=cut

sub run {
	my $self = shift;
	my $dbh = DBI->connect(
		$self->parent->dsn,
		$self->parent->user,
		$self->parent->pass,
		$self->parent->options
	);

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

