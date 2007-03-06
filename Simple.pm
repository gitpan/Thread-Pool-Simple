package Thread::Pool::Simple;
use 5.008;
use strict;
use threads;
use threads::shared;
use warnings;
use Carp;
use Storable qw(nfreeze thaw);
use Thread::Queue;

our $VERSION = '0.02';

sub new {
    my $class = shift;
    my %arg = @_;
    my %config : shared = (min => 1,
                           max => 10,
                           load => 100,
                           lifespan => 1000,
                          );
    for ('min', 'max', 'load', 'lifespan') {
        $config{$_} = $arg{$_} if exists $arg{$_};
    }
    my %code_ref;
    $code_ref{pre} = $arg{pre} || [];
    $code_ref{do} = $arg{do} || [];
    $code_ref{post} = $arg{post} || [];

    my %obj : shared;
    my $self = \%obj;
    $self->{config} = \%config;
    $self->{pending} = Thread::Queue->new();
    $self->{done} = &share({});
    $self->{worker} = &share({});
    $self->{worker}{count} = 0;
    my $state = 1;
    $self->{state} = &share(\$state);
    bless $self, $class;
    $self->{thread} = &share({});
    async {
        $self->_run(\%code_ref);
    }->detach();
    return $self;
}

sub _run {
    my ($self, $code_ref) = @_;
    while (1) {
        {
            if (!$self->_state()) {
                sleep 1;
                next;
            }
        }
        last if $self->terminating();
        $self->increase($code_ref) if $self->busy();
        threads->yield();
    }
    my $worker = $self->{worker};
    lock %$worker;
    --$worker->{count};
    cond_signal %$worker;
}

sub _handle {
    my ($self, $code_ref) = @_;
    my $lifespan = do { lock %{$self->{config}}; $self->{config}{lifespan} };
    while (1) {
        {
            if (!$self->_state()) {
                sleep 1;
                redo;
            }
        }
        last unless $lifespan-- && !$self->terminating();
        my $do_code = $code_ref->{do};
        next unless 'CODE' eq ref $do_code->[0];
        my ($id, $job) = unpack('Na*', $self->{pending}->dequeue());
        last if $id == 1;
        my $arg = thaw($job);
        my @ret;
        if ($id == 0) {
            eval { scalar $do_code->[0](splice(@$do_code, 1), @$arg) };
            next;
        }
        elsif ($id % 2) {
            $ret[0] = eval { $do_code->[0](splice(@$do_code, 1), @$arg) };
        }
        else {
            @ret = eval { $do_code->[0](splice(@$do_code,1), @$arg) };
        }
        $ret[0] = $@ if $@;
            my $ret = nfreeze(\@ret);
        {
            lock %{$self->{done}};
            $self->{done}{$id} = $ret;
            cond_signal %{$self->{done}};
        }
        threads->yield();
    }
    my $post_code = $code_ref->{post};
    if ('CODE' eq ref $post_code->[0]) {
        eval { $post_code->[0](splice(@$post_code, 1)) };
        carp $@ if $@;
    }
    my $worker = $self->{worker};
    lock %$worker;
    --$worker->{count};
    cond_signal %$worker;
}

sub join {
    my ($self) = @_;
    $self->_state(-1);
    my $worker = $self->{worker};
    lock %$worker;
    $self->{pending}->enqueue((pack('Na*', 1, '')) x $worker->{count});
    cond_wait %$worker while $worker->{count} >= 0;
}

sub detach {
    my ($self) = @_;
    $self->_state(-1);
    my $worker = $self->{worker};
    lock %$worker;
    $self->{pending}->enqueue((pack('Na*', 1, '')) x $worker->{count});
}

sub pause {
    my ($self) = @_;
    return unless $self->_state() > 0;
    $self->_state(0);
}

sub resume {
    my ($self) = @_;
    return if $self->_state();
    $self->_state(1);
}

sub running {
    my ($self) = @_;
    return if $self->_state() > 0;
}

sub terminating {
    my ($self) = @_;
    my $state = $self->_state();
    return unless $state < 0;
    my $pending = $self->{pending}->pending();
    return 1 if $state == -2 && !$pending;
    my $done = do { lock %{$self->{done}}; keys %{$self->{done}} };
    return 1 if $state == -1 && !$pending && !$done;
    return;
}

sub increase {
    my ($self, $code_ref) = @_;
    eval {
        my $worker = $self->{worker};
        lock %$worker;
        my $max = do { lock %{$self->{config}}; $self->{config}{max} };
        return if $worker->{count} > $max;
        my $pre_code = $code_ref->{pre};
        if ('CODE' eq ref $pre_code->[0]) {
            eval { $pre_code->[0](splice(@$pre_code, 1)) };
            carp $@ if $@;
        }
        threads->create(\&_handle, $self, $code_ref)->detach();
        ++$worker->{count};
    };
    carp "fail to add new thread: $@" if $@;
}

sub busy {
    my ($self) = @_;
    my $worker = do { lock %{$self->{worker}}; $self->{worker}{count} };
    my ($min, $load) = do { lock %{$self->{config}}; @{$self->{config}}{'min', 'load'} };
    return $worker < $min
      || $self->{pending}->pending() > $worker * $load;
}

sub _state {
    my $self = shift;
    my $state = $self->{state};
    lock $$state;
    return $$state unless @_;
    my $s = shift;
    $$state = $s;
    return $$state;
}

sub config {
    my $self = shift;
    my $config = $self->{config};
    lock %$config;
    return %$config unless @_;
    %$config = (%$config, @_);
    return %$config;
}

sub add {
    my $self = shift;
    my $context = wantarray;
    my $arg = nfreeze(\@_);
    my $id = 0;
    while (1) {
        $id = int(rand(time())) if defined $context;
        next if defined $context && $id < 10;
        ++$id if defined $context && $context == $id % 2;
        lock %{$self->{done}};
        last unless exists $self->{done}{$id};
    }
    $self->{pending}->enqueue(pack('Na*', $id, $arg));
    return $id;
}

sub remove {
    my ($self, $id) = @_;
    return unless $id;
    lock %{$self->{done}};
    cond_wait %{$self->{done}} until exists $self->{done}{$id};
    cond_signal %{$self->{done}} if 1 < keys %{$self->{done}};
    my $ret = delete $self->{done}{$id};
    return unless defined $ret;
    $ret = thaw($ret);
    return $ret->[0] if $id % 2;
    return @$ret;
}

sub remove_nb {
    my ($self, $id) = @_;
    return unless $id;
    lock %{$self->{done}};
    my $ret = delete $self->{done}{$id};
    return unless defined $ret;
    $ret = thaw($ret);
    return ($id, $ret->[0]) if $id % 2;
    return ($id, @$ret);
}

1;
__END__

=head1 NAME

Thread::Pool::Simple - A simple thread-pool implementation

=head1 SYNOPSIS

  use Thread::Pool::Simple;

  my $pool = Thread::Pool::Simple->new(
                 min => 3,           # at least 3 workers
                 max => 5,           # at most 5 workers
                 load => 10,         # increase worker if on average every worker has 10 jobs waiting
                 lifespan => 1000,   # work retires after 1000 jobs
                 pre => \&pre_handle, # run before creating worker thread
                 do => \&do_handle,   # job handler for each worker
                 post => \&post_handle, # run after worker threads end
               );

  my ($id1) = $pool->add(@arg1); #call in list context
  my $id2) = $pool->add(@arg2);  #call in scalar conetxt
  $pool->add(@arg3)              #call in void context

  my @ret = $pool->remove($id1); #get result (block)
  my $ret = $pool->remove_nb($id2); #get result (no block)

  $pool->join();                 #wait till all jobs are done
  $pool->detach();               #don't wait.

=head1 DESCRIPTION

C<Thread::Pool::Simple> provides a simple thread-pool implementaion
without external dependencies outside core modules.

Jobs can be submitted to and handled by multi-threaded `workers'
managed by the pool.

=head1 AUTHOR

Jianyuan Wu, E<lt>jwu@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2007 by Jianyuan Wu

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
