requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.21';
requires 'Try::Tiny', 0;
requires 'IO::Async', '>= 0.60';
requires 'DBI', 0;

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
	requires 'Test::Fatal', '>= 0.010';
};


