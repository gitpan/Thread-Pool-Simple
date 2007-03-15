package Thread::Pool::Simple;
use 5.008;
use strict;
use threads;
use threads::shared;
use warnings;
use Carp;
use Storable qw(nfreeze thaw);
use Thread::Queue;
use Thread::Semaphore;

our $VERSION = '0.10';

sub new {
    my $class = shift;
    my %arg = @_;
    my %config : shared = (min => 1,
                           max => 10,
                           load => 20,
                           lifespan => 50000,
                           passid => 0,
                          );
    for ('min', 'max', 'load', 'lifespan', 'passid') {
        $config{$_} = $arg{$_} if defined $arg{$_};
    }
    my %handler;
    $handler{pre} = $arg{pre} || [];
    $handler{do} = $arg{do} || [];
    $handler{post} = $arg{post} || [];

    my $self = &share({});
    $self->{config} = \%config;
    $self->{pending} = Thread::Queue->new();
    $self->{job} = &share({});
    $self->{done} = &share({});
    my $state : shared = 0;
    $self->{state} = \$state;
    my $worker : shared = 0;
    $self->{worker} = \$worker;
    $self->{run_lock} = Thread::Semaphore->new();
    bless $self, $class;
    async {
        $self->_run(\%handler);
    }->detach();
    return $self;
}

sub _run {
    my ($self, $handler) = @_;
    $self->{run_lock}->down();
    while (1) {
        last if $self->terminating();
        $self->increase($handler) if $self->busy();
        threads->yield();
    }
    my $worker = $self->{worker};
    lock $$worker;
    cond_wait $$worker while $$worker;
    sleep 1;
    $self->{run_lock}->up();
}

sub _handle {
    my ($self, $handler) = @_;
    my $do = $handler->{do};
    my $func = shift @$do;
    my ($lifespan, $passid) = do { lock %{$self->{config}}; @{$self->{config}}{'lifespan', 'passid'} };
    eval {
        no strict 'refs';
        while (!$self->terminating()
               && $lifespan--
              ) {
            my ($id, $job) = unpack('Na*', $self->{pending}->dequeue());
            if (!$id) {
                $self->_state(-2);
                last;
            }
            if (!$self->job_valid($id)) {
                $self->_remove_job($id);
                next;
            }
            my $arg = thaw($job);
            my @ret;
            if ($id % 3 == 2) {
                if (defined $func) {
                    eval { scalar $func->($passid ? ($id, @$do, @$arg) : (@$do, @$arg)) };
                }
                $self->_remove_job($id);
                next;
            }
            elsif ($id % 3 == 1) {
                if (defined $func) {
                    @ret = eval { $func->($passid ? ($id, @$do, @$arg) : (@$do, @$arg)) };
                }
            }
            else {
                if (defined $func) {
                    $ret[0] = eval { $func->($passid ? ($id, @$do, @$arg) : (@$do, @$arg)) };
                }
            }
            $ret[0] = $@ if $@;
            my $ret = nfreeze(\@ret);
            if ($self->job_valid($id)) {
                lock %{$self->{done}};
                $self->{done}{$id} = $ret;
                cond_signal %{$self->{done}};
            }
            else {
                $self->_remove_job($id);
            }
        }
        continue {
            threads->yield();
        }
    };
    carp "fatal error: $@" if $@;
    my $post = $handler->{post};
    $func = shift @$post;
    if (defined $func) {
        eval {
            no strict 'refs';
            $func->(@$post);
        };
        carp $@ if $@;
    }
    my $worker = $self->{worker};
    lock $$worker;
    --$$worker;
    cond_signal $$worker;
}

sub _state {
    my $self = shift;
    my $state = $self->{state};
    lock $$state;
    return $$state unless @_;
    my $s = shift;
    $$state = $s;
    return $s;
}

sub join {
    my ($self, $nb) = @_;
    $self->_state(-1);
    my $max = do { lock %{$self->{config}}; $self->{config}{max} };
    $self->{pending}->enqueue((pack('Na*', 0, '')) x $max);
    return if $nb;
    $self->{run_lock}->down();
}

sub detach {
    my ($self) = @_;
    $self->join(1);
}

sub increase {
    my ($self, $handler) = @_;
    my $max = do { lock %{$self->{config}}; $self->{config}{max} };
    my $worker = $self->{worker};
    lock $$worker;
    return unless $$worker < $max;
    my $pre = $handler->{pre};
    my $func = shift @$pre;
    if (defined $func) {
        eval {
            no strict 'refs';
            $func->(@$pre);
        };
        carp $@ if $@;
    }
    eval {
        threads->create(\&_handle, $self, $handler)->detach();
        ++$$worker;
    };
    carp "fail to add new thread: $@" if $@;
}

sub busy {
    my ($self) = @_;
    my $worker = do { lock ${$self->{worker}}; ${$self->{worker}} };
    my ($min, $load) = do { lock %{$self->{config}}; @{$self->{config}}{'min', 'load'} };
    return $worker < $min
      || $self->{pending}->pending() > $worker * $load;
}

sub terminating {
    my ($self) = @_;
    my $state = $self->_state();
    my $job = do { lock %{$self->{job}}; keys %{$self->{job}} };
    return 1 if $state == -1 && !$job;
    return 1 if $state == -2;
    return;
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
    $context = 2 unless defined $context; # void context = 2
    my $arg = nfreeze(\@_);
    my $id;
    while (1) {
        $id = int(rand(time()));
        next unless $id;
        ++$id unless $context == $id % 3;
        ++$id unless $context == $id % 3;
        lock %{$self->{job}};
        next if exists $self->{job}{$id};
        $self->{pending}->enqueue(pack('Na*', $id, $arg));
        $self->{job}{$id} = 1;
        last;
    }
    return $id;
}

sub job_valid {
    my ($self, $id) = @_;
    lock %{$self->{job}};
    return $self->{job}{$id};
}

sub _remove_job {
    my ($self, $id) = @_;
    lock %{$self->{job}};
    delete $self->{job}{$id};
}

sub _remove {
    my ($self, $id, $nb) = @_;
    return if $id % 3 == 2;
    return unless $self->job_valid($id);
    my ($exist, $ret);
    {
        lock %{$self->{done}};
        if (!$nb) {
            cond_wait %{$self->{done}} until exists $self->{done}{$id};
            cond_signal %{$self->{done}} if 1 < keys %{$self->{done}};
        }
        $exist = ($ret) = delete $self->{done}{$id};
    }
    $self->_remove_job($id) if $exist;
    return $exist unless defined $ret;
    $ret = thaw($ret);
    return ($exist, @$ret) if $id % 3 == 1;
    return ($exist, $ret->[0]);
}

sub remove {
    my ($self, $id) = @_;
    my ($exist, @ret) = $self->_remove($id);
    return @ret;
}


sub remove_nb {
    my ($self, $id) = @_;
    return $self->_remove($id, 1);
}

sub cancel {
    my ($self, $id) = @_;
    my ($exist) = $self->remove_nb($id);
    if ($exist) {
        $self->_remove_job($id);
    }
    else {
        lock %{$self->{job}};
        $self->{job}{$id} = 0;
    }
}

sub cancel_all {
    my ($self) = @_;
    my @id = do { lock %{$self->{job}}; keys %{$self->{job}} };
    for (@id) {
        $self->cancel($_);
    }
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
                 pre => [\&pre_handle, $arg1, $arg2, ...]   # run before creating worker thread
                 do => [\&do_handle, $arg1, $arg2, ...]     # job handler for each worker
                 post => [\&post_handle, $arg1, $arg2, ...] # run after worker threads end
                 passid => 1,        # whether to pass the job id as the first argument to the &do_handle
                 lifespan => 10000,  # total jobs handled by each worker
               );

  my ($id1) = $pool->add(@arg1); # call in list context
  my $id2 = $pool->add(@arg2);   # call in scalar conetxt
  $pool->add(@arg3)              # call in void context

  my @ret = $pool->remove($id1); # get result (block)
  my $ret = $pool->remove_nb($id2); # get result (no block)

  $pool->cancel($id1);           # cancel the job
  $pool->cancel_all();           # cancel all jobs

  $pool->join();                 # wait till all jobs are done
  $pool->detach();               # don't wait.

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
