package DBIx::Async::Handle;
use strict;
use warnings;

=head1 NAME

DBIx::Async::Handle - statement handle for L<DBIx::Async>

=head1 DESCRIPTION

=head1 METHODS

=cut

=head2 new

Returns $self.

=cut

sub new { my $class = shift; bless { @_ }, $class }

=head2 dbh

Returns $self.

=cut

sub dbh { shift->{dbh} }

=head2 execute

Returns $self.

=cut

sub execute {
	my $self = shift;
	$self->{execute} = $self->{prepare}->and_then(sub {
		my $id = shift->get->{id};
		$self->dbh->queue({ op => 'execute', id => $id });
	});
}

=head2 fetchrow_hashref

Returns $self.

=cut

sub fetchrow_hashref {
	my $self = shift;
	$self->{execute}->and_then(sub {
		my $id = shift->get->{id};
#		warn "Fetch row with ID $id\n";
		$self->dbh->queue({
			op => 'fetchrow_hashref',
			id => $id
		});
	})->and_then(sub {
		my $response = shift->get;
		Future->new->done($response->{data} // ());
	});
}

=head2 iterate

Returns $self.

=cut

sub iterate {
	my $self = shift;
	my $method = shift;
	my $code = shift;
	my $f = Future->new;
	my $step;
	$step = sub {
		return $f->done unless @_;
		$code->(@_);
		$self->$method->on_done($step);
	};
	$self->$method->on_done($step);
	$f;
}

1;

__END__

=head1 AUTHOR

Tom Molesworth <cpan@entitymodel.com>

=head1 LICENSE

Copyright Tom Molesworth 2012-2013. Licensed under the same terms as Perl itself.

