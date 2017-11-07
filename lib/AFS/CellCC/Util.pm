# Copyright (c) 2015, Sine Nomine Associates
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

package AFS::CellCC::Util;

use strict;
use warnings;

use Digest;
use Log::Log4perl qw(:easy);
use File::stat;
use Filesys::Df;

use AFS::CellCC::Config qw(config_get);
use AFS::CellCC::DB qw(update_job);

use POSIX qw(WNOHANG WIFSIGNALED WTERMSIG WIFEXITED WEXITSTATUS);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(describe_file spawn_child monitor_child pretty_bytes scratch_ok
                    calc_checksum get_ids scratch_ok_jobs calc_checksum_jobs);

# Return a "pretty" human-readable modification of a number of bytes. e.g.
# "1.00 MB" instead of "1048576".
sub
pretty_bytes($) {
    my ($bytes) = @_;
    for my $suffix (' bytes', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB') {
        if ($bytes < 1024) {
            return sprintf("%.2f", $bytes) . "$suffix";
        }
        $bytes /= 1024;
    }
    # Something is wrong, but don't die just for a pretty human-readable thing
    return "unknown bytes";
}

# This returns a pair of strings like:
#     ("2.03 MB", "400.20 kB/s")
# which indicates the size of a file, and how fast it is growing. The caller
# provides the filename (which we stat), and scalar refs to store the previous
# size and previous timestamp for the last time this sub was called.
sub
describe_file($$$) {
    my ($filename, $bytesref, $timeref) = @_;

    my ($last_bytes, $last_time) = ($$bytesref, $$timeref);

    my $sb = stat($filename);
    my $now = time();

    my $pretty_bytes = pretty_bytes($sb->size);
    my $pretty_rate = "unknown bytes/s";
    if (defined($last_bytes) && defined($last_time)) {
        my $rate = int(($sb->size - $last_bytes)/($now - $last_time));
        if ($rate >= 0) {
            $pretty_rate = pretty_bytes($rate) . "/s";
        }
    }

    $$bytesref = $sb->size;
    $$timeref = $now;

    return ($pretty_bytes, $pretty_rate);
}

# Run a command in a child process, and return the pid for that process.
# Arguments:
#  - exec: An arrayref of arguments to run. e.g. ['/bin/echo', 'foo', 'bar']
#  - cb: Instead of running a command, call this in the child process
#  - name: A human-readable name to describe this process, used in log messages
#  - stdout: A filename to store the stdout of the child process
#  - stderr: A filename to store the stderr of the child process
sub
spawn_child(%) {
    my (%opts) = @_;

    if ($opts{exec}) {
        DEBUG "spawning child command: ".(join(' ', @{ $opts{exec} }));
    }

    my $pid = fork();
    if (!defined($pid)) {
        die("Cannot fork: $!\n");
    }

    if ($pid == 0) {
        # Child
        eval {
            if ($opts{stdout}) {
                open(STDOUT, '>', $opts{stdout});
            }
            if ($opts{stderr}) {
                open(STDERR, '>', $opts{stderr});
            }

            if ($opts{cb}) {
                $opts{cb}->();
            } elsif ($opts{exec}) {
                my $bin = $opts{exec}->[0];
                exec { $bin } @{ $opts{exec} };
                die("Exec of $bin failed: $!\n");
            }
        };
        if ($@) {
            warn $@;
            exit(1);
        }
        exit(0);
    }

    # Parent

    if (defined($opts{name})) {
        DEBUG "Spawned ".$opts{name}." in pid $pid";
    }

    return $pid;
}

# We can get interrupted by a SIGCHLD while sleeping
sub
_interruptible_sleep($) {
    my ($sec) = @_;

    local $SIG{CHLD} = sub {};

    sleep($sec);
}

# Monitor a spawned child process. Takes a pid and some options in a hashref.
# Available options:
#  - name: A human-readable name for what we're waiting for; used in log messages
#  - error_fh: A filehandle to the child's stderr or similar. This is examined
#              for errors if the child exits uncleanly.
#  - cb: A callback to call to monitor the child process. It is given one
#        argument, which is the approximate number of seconds we'll wait before
#        'cb' is called again.
#  - cb_intervals: An arrayref of integers, which indicate a number of seconds
#                  to wait before looking at the child process. For example
#                  [1,2,3] means to wait 1 second, then 2 seconds, then 3
#                  seconds, in between monitoring the child process. After that,
#                  we keep waiting for 3 seconds repeatedly until the child
#                  process stop.
sub
monitor_child($$) {
    my ($pid, $opts) = @_;
    my $name = "pid $pid";
    my @cbintervals = (2, 30);
    my $cur_cbinterval;
    my $child_dead = 0;

    if ($opts->{name}) {
        $name = $opts->{name};
    }
    if ($opts->{cb_intervals}) {
        @cbintervals = @{$opts->{cb_intervals}};
    }

    if (!@cbintervals) {
        die("Empty set of intervals provided; you must specify at least one\n");
    }

    $cur_cbinterval = shift @cbintervals;

    my $last_cb = time();

    eval { while (1) {
        my $res = waitpid($pid, WNOHANG);
        if ($res < 0) {
            die("Error waiting for $name: $!\n");
        }

        if ($res > 0) {
            # Child process has exited
            $child_dead = 1;
            my $signal = WIFSIGNALED($?) ? 'signal '.WTERMSIG($?) : '';
            my $exit_status = WIFEXITED($?) ? 'exit code '.WEXITSTATUS($?) : '';
            if ($?) {
                die("'$name' died with an error ($signal$exit_status)\n");
            }
            return;
        }

        # Child is still running. Call our callback function if enough time
        # has elapsed.
        if ($opts->{cb}) {
            my $now = time();
            if ($now - $last_cb >= $cur_cbinterval) {
                if (@cbintervals) {
                    $cur_cbinterval = shift @cbintervals;
                }
                $opts->{cb}->($cur_cbinterval);
                $last_cb = $now;
            }
        }

        _interruptible_sleep(1);
    }};
    if ($@) {
        my $error = $@;
        if ($child_dead) {
            my $error_fh = $opts->{error_fh};
            if ($error_fh) {
                # See if the child printed anything to stderr; that can be useful
                # to see why it quit
                seek($error_fh, 0, 0);
                while (<$error_fh>) {
                    chomp;
                    my $line = $_;
                    if ($line) {
                        WARN "$name stderr: $line";
                    }
                }
            }
        } else {
            # Tell the child to quit, if we're not going to be around to monitor
            # it anymore.
            kill('TERM', $pid);
        }
        die($error);
    }
}

# Is the argument a positive integer? (that is, are all characters digits?)
sub
_is_number($) {
    my ($num) = @_;
    if ($num =~ m/^\d+$/) {
        return 1;
    }
    return 0;
}

# Take a "pretty" description of bytes, and get the actual amount as just a
# plain integer. e.g. converts 1M to 1048576.
sub
_unpretty_bytes($) {
    my ($pretty_bytes) = @_;

    # Create a mapping for K => 1024, M => 1024*1024, etc
    my @suffixes = qw(K M G T P E Z);
    my %conv;
    my $multiplier = 1024;
    for (@suffixes) {
        $conv{$_} = $multiplier;
        $multiplier *= 1024;
    }

    my $num = $pretty_bytes;
    my $suffix = chop $num;

    if (exists $conv{$suffix}) {
        $multiplier = $conv{$suffix};
        if (!_is_number($num)) {
            die("Config error: Value '$num' is supposed to be a number, but does not look like one");
        }
        return $num * $multiplier;
    }

    if (!_is_number($pretty_bytes)) {
        die("Config error: Value '$pretty_bytes' is supposed to be a number, but does not look like one");
    }
    return $pretty_bytes;
}

# When there is not enough space free to store a dump blob on the local scratch
# disk, call this to revert the job to the given state.
sub
_scratch_rollback($$) {
    my ($job, $state) = @_;
    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               to_state => $state,
               timeout => undef,
               description => "Waiting for enough scratch disk space to be free");
}

# This checks if there is enough disk space free in a scratch directory. If
# there is not enough space, we log a warning and roll back the job to a known
# state we can retry.
sub
scratch_ok($$$$$) {
    my ($job, $prev_state, $size, $scratch_dir, $scratch_min) = @_;

    if (!defined($scratch_min)) {
        DEBUG "skipping scratch_ok check";
        return 1;
    }

    my $pretty_size = pretty_bytes($size);

    $scratch_min = _unpretty_bytes($scratch_min);

    my $statfs = df($scratch_dir)
        or die("Cannot get filesystem info for $scratch_dir: $!\n");

    my $bytes_free = $statfs->{bfree} * 1024;
    if ($size > $bytes_free) {
        my $pretty_free = pretty_bytes($bytes_free);

        WARN "job $job->{jobid} needs $size in $scratch_dir, but only ".
             "$pretty_free are free";
        WARN "Not proceeding with job $job->{jobid}";
        _scratch_rollback($job, $prev_state);
        return 0;
    }

    my $bytes_left = $bytes_free - $size;
    if ($bytes_left < $scratch_min) {
        my $pretty_left = pretty_bytes($bytes_left);
        my $pretty_min = pretty_bytes($scratch_min);

        WARN "job $job->{jobid} ($pretty_size) would leave only $pretty_left ".
             "free in $scratch_dir, but we need $pretty_min";
        WARN "Not proceeding with job $job->{jobid}";
        _scratch_rollback($job, $prev_state);
        return 0;
    }

    $bytes_left .= " (".pretty_bytes($bytes_left).")";
    $scratch_min .= " (".pretty_bytes($scratch_min).")";

    DEBUG "job $job->{jobid} size $size leaves scratch dir $scratch_dir with ".
          "$bytes_left free space, which is more than the configured minimum of ".
          "$scratch_min";

    return 1;
}

# Same as scratch_ok but receives several jobs. The destination cells of the
# jobs received as an argument are requesting the same volume. Knowing that, we
# dump the volume only once and create hardlinks for the jobs. Since the size
# of the volume should be much greater than the size of the hardlinks, and
# assuming we should not have that many destination cells, the size of the
# hardlinks are not taken into consideration.
sub
scratch_ok_jobs($$$$@) {
    my ($prev_state, $size, $scratch_dir, $scratch_min, @jobs) = @_;

    if (!defined($scratch_min)) {
        DEBUG "skipping scratch_ok_jobs check";
        return 1;
    }

    my $pretty_size = pretty_bytes($size);

    $scratch_min = _unpretty_bytes($scratch_min);

    my $statfs = df($scratch_dir)
        or die("Cannot get filesystem info for $scratch_dir: $!\n");

    my $bytes_free = $statfs->{bfree} * 1024;
    if ($size > $bytes_free) {
        my $pretty_free = pretty_bytes($bytes_free);

        for my $job (@jobs) {
            WARN "job $job->{jobid} needs $size in $scratch_dir, but only ".
                 "$pretty_free are free";
            WARN "Not proceeding with job $job->{jobid}";
            _scratch_rollback($job, $prev_state);
        }
        return 0;
    }

    my $bytes_left = $bytes_free - $size;
    if ($bytes_left < $scratch_min) {
        my $pretty_left = pretty_bytes($bytes_left);
        my $pretty_min = pretty_bytes($scratch_min);

        for my $job (@jobs) {
            WARN "job $job->{jobid} ($pretty_size) would leave only $pretty_left ".
                 "free in $scratch_dir, but we need $pretty_min";
            WARN "Not proceeding with job $job->{jobid}";
            _scratch_rollback($job, $prev_state);
        }
        return 0;
    }

    $bytes_left .= " (".pretty_bytes($bytes_left).")";
    $scratch_min .= " (".pretty_bytes($scratch_min).")";

    my $ids = get_ids(@jobs);
    DEBUG "jobs $ids size $size leaves scratch dir $scratch_dir with ".
          "$bytes_left free space, which is more than the configured minimum of ".
          "$scratch_min";

    return 1;
}

# Return the checksum of the file received as an argument.
# The checksum returned by this function will be in following
# format: "algorithm:checksum", where algorithm is one of
# the algorithms defined by the module Digest (e.g. MD5) and
# checksum is the checksum of the file received as an argument.
# This checksum will be in hexadecimal form.
sub
calc_checksum($$$$$$) {
    my ($dumpfh, $filesize, $algo, $jobid, $dvref, $state) = @_;

    if ($filesize < 0) {
        die("The size of the file ($filesize) cannot be negative\n");
    }

    my $start = time();
    my $now;

    my $total = pretty_bytes($filesize);
    my $bytes;
    my $nbytes;

    my $digest = Digest->new($algo);
    my $checksum;

    my $buf;
    my $descr;
    my $pos;

    while (1) {
        $nbytes = read($dumpfh, $buf, 16384);

        if ($nbytes == 0) {
            last;
        }
        if (!defined($nbytes)) {
            die("Read of dump file failed: $!\n");
        }
        $digest->add($buf);

        $now = time();
        if ($now < $start || $now - $start > 60) {
            $pos = tell($dumpfh);

            if ($pos < 0) {
                die("'tell' failed: $!\n");
            }
            $bytes = pretty_bytes($pos);
            $descr = "Checksumming dump blob ($bytes / $total)";

            update_job(jobid => $jobid,
                       dvref => $dvref,
                       to_state => $state,
                       timeout => 120,
                       description => $descr);
            $start = time();
        }
    }
    $checksum = $digest->hexdigest;
    return "$algo:$checksum";
}

sub
calc_checksum_jobs($$$$@) {
    my ($dumpfh, $filesize, $algo, $state, @jobs) = @_;

    if ($filesize < 0) {
        die("The size of the file ($filesize) cannot be negative\n");
    }

    my $start = time();
    my $now;

    my $total = pretty_bytes($filesize);
    my $bytes;
    my $nbytes;

    my $digest = Digest->new($algo);
    my $checksum;

    my $buf;
    my $descr;
    my $pos;

    while (1) {
        $nbytes = read($dumpfh, $buf, 16384);

        if ($nbytes == 0) {
            last;
        }
        if (!defined($nbytes)) {
            die("Read of dump file failed: $!\n");
        }
        $digest->add($buf);

        $now = time();
        if ($now < $start || $now - $start > 60) {
            $pos = tell($dumpfh);

            if ($pos < 0) {
                die("'tell' failed: $!\n");
            }
            $bytes = pretty_bytes($pos);
            $descr = "Checksumming dump blob ($bytes / $total)";

            for my $job (@jobs) {
                update_job(jobid => $job->{jobid},
                           dvref => \$job->{dv},
                           to_state => $state,
                           timeout => 120,
                           description => $descr);
            }
            $start = time();
        }
    }
    $checksum = $digest->hexdigest;
    return "$algo:$checksum";
}

sub
get_ids(@) {
    my (@jobs) = @_;
    my @ids;
    for my $job (@jobs) {
        push(@ids, $job->{jobid});
    }
    return join( ',', @ids );
}

1;
