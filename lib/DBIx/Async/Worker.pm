package DBIx::Async::Worker;
use strict;
use warnings;

=head1 NAME

DBIx::Async::Worker

=cut

use DBI;

use Try::Tiny;

my %VALID_METHODS;
BEGIN {
	@VALID_METHODS{qw(
		begin_work commit rollback
		do prepare execute fetchrow_hashref finish
	)} = ();
}

my %sth;

=head1 METHODS

=cut

=head2 new

Returns $self.

=cut

sub new { my $class = shift; bless { @_ }, $class }

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

sub connect {
	my $self = shift;
	$self->{dbh} = DBI->connect(
		$self->parent->dsn,
		$self->parent->user,
		$self->parent->pass,
		$self->parent->options
	);
	$self;
}

sub do : method {
	my $self = shift;
	my $op = shift;
	$self->dbh->do($op->{sql});
	return { status => 'ok' };
}

sub begin_work {
	my $self = shift;
	my $op = shift;
	$self->dbh->begin_work;
	return { status => 'ok' };
}

sub commit {
	my $self = shift;
	my $op = shift;
	$self->dbh->commit;
	return { status => 'ok' };
}

sub rollback {
	my $self = shift;
	my $op = shift;
	$self->dbh->rollback;
	return { status => 'ok' };
}

sub prepare {
	my $self = shift;
	my $op = shift;
	my $sth = $self->dbh->prepare($op->{sql});
	$sth{$sth} = $sth;
	return { status => 'ok', id => "$sth" };
}

sub execute {
	my $self = shift;
	my $op = shift;
	my $sth = $sth{$op->{id}} or return {
		status => 'fail',
		message => 'invalid ID'
	};
	$sth->execute(@{ $op->{param} });
	return { status => 'ok', id => "$sth" };
}

sub fetchrow_hashref {
	my $self = shift;
	my $op = shift;
	my $sth = $sth{$op->{id}} or return {
		status => 'fail',
		message => 'invalid ID'
	};
	my $data = $sth->fetchrow_hashref;
	return { status => 'ok', id => "$sth", data => $data };
}

sub run {
	my $self = shift;
	$self->connect;
	$self->setup;
	while(my $data = $self->sth_ch->recv) {
		try {
			my $method = $data->{op};
			my $code = $self->can($method) or die 'unknown operation';
			$self->ret_ch->send($code->($self, $data));
		} catch {
			$self->ret_ch->send({ status => 'fail', message => $_ });
		};
	}
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2012-2013. Licensed under the same terms as Perl itself.
