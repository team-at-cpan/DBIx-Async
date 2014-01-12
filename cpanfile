requires 'parent', 0;
requires 'curry', 0;
requires 'Future', '>= 0.20';
requires 'Try::Tiny', 0;
requires 'Mixin::Event::Dispatch', '>= 1.002';
requires 'IO::Async', '>= 0.60';

on 'test' => sub {
	requires 'Test::More', '>= 0.98';
	requires 'Test::Fatal', '>= 0.010';
};


