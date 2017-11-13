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

package AFS::CellCC::Dump;

use strict;
use warnings;

use 5.008_000;

use File::Basename;
use File::stat;
use File::Temp;

# This turns on the DEBUG, INFO, WARN, ERROR functions
use Log::Log4perl qw(:easy);

use AFS::CellCC;
use AFS::CellCC::Config qw(config_get);
use AFS::CellCC::DB qw(db_rw find_update_jobs update_job job_error);
use AFS::CellCC::VOS qw(vos_auth find_volume volume_exists volume_times);
use AFS::CellCC::Util qw(spawn_child monitor_child describe_file pretty_bytes scratch_ok
                         calc_checksum get_ids scratch_ok_jobs calc_checksum_jobs);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(process_dumps);

# Do what is needed after a volume dump has been successfully generated. We
# currently just calculate a checksum for the dump blob, and update the
# database to give the dump filename and metadata.
sub
_dump_success($$$) {
    my ($prev_state, $jobs_ref, $dump_fhs_ref) = @_;
    my @jobs = @{$jobs_ref};
    my @dump_fhs = @{$dump_fhs_ref};
    my $done_state = 'DUMP_DONE';
    my $filesize = stat($dump_fhs[0])->size;

    seek($dump_fhs[0], 0, 0);

    foreach my $i (1 .. $#dump_fhs) {
        File::Copy::copy($dump_fhs[0]->filename, $dump_fhs[$i]->filename);
    }
    # Note that this checksum doesn't need to by cryptographically secure. md5
    # should be fine.
    my $algo = config_get('dump/checksum');
    my $checksum = calc_checksum_jobs($dump_fhs[0], $filesize, $algo, $prev_state, @jobs);

    foreach my $i (0 .. $#jobs) {
        update_job(jobid => $jobs[$i]->{jobid},
                   dvref => \$jobs[$i]->{dv},
                   from_state => $prev_state,
                   to_state => $done_state,
                   dump_fqdn => config_get('fqdn'),
                   dump_method => 'remctl',
                   dump_port => config_get('remctl/port'),
                   dump_filename => basename($dump_fhs[$i]->filename),
                   dump_checksum => $checksum,
                   dump_filesize => $filesize,
                   timeout => undef,
                   description => "Waiting to xfer dump file");
    }

    # Keep the dump file around; we've reported to the db that we have it.
    for my $dump_fh (@dump_fhs) {
        $dump_fh->unlink_on_destroy(0);
    }
}

# Get the estimated dump size for the given volume.
sub
_get_size($$$$$) {
    my ($job, $volname, $server, $partition, $lastupdate) = @_;

    my $vos = vos_auth();
    my $result = $vos->size(id => $volname, dump => 1, time => $lastupdate)
        or die("vos size error: ".$vos->errors());
    my $size = $result->dump_size;
    if (!defined($size)) {
        die;
    }
    return $size;
}

# Returns a timestamp if we should dump from that timestamp (0 for a full dump,
# or the timestamp from which we should be dumping incremental changes). Or,
# returns undef if we should not sync the volume at all.
sub
_calc_incremental($$) {
    my ($job, $state) = @_;

    if (!config_get('dump/incremental/enabled')) {
        DEBUG "Not checking remote volume; incremental dumps are not configured";
        return 0;
    }

    my $skip_unchanged = 0;
    if (config_get('dump/incremental/skip-unchanged')) {
        $skip_unchanged = 1;
    }

    my $error_fail = 1;
    if (config_get('dump/incremental/fulldump-on-error')) {
        $error_fail = 0;
    }

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $state,
               timeout => 1200,
               description => "Checking remote volume metadata");

    if (!volume_exists($job->{volname}, $job->{dst_cell})) {
        # Volume does not exist, so we'll be doing a full dump
        return 0;
    }

    my $remote_times;
    my $local_times;
    eval {
        $remote_times = volume_times($job->{volname}, $job->{dst_cell});

        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $state,
                   timeout => 1200,
                   description => "Checking local volume metadata");

        $local_times = volume_times($job->{volname}, $job->{src_cell});

        if ($remote_times->{update} > $local_times->{update}) {
            # The remote volume appears to have data newer than the local
            # volume? That doesn't make any sense...
            die("Weird times on volume $job->{volname}: remote update ".
                "$remote_times->{update} local update $local_times->{update}\n");
        }
    };
    if ($@) {
        if ($error_fail) {
            die("Error when getting metadata for remote volume $job->{volname}: $@\n");
        }
        WARN "Encountered error when fetching metadata for remote volume ".
             "$job->{volname} (jobid $job->{jobid}), forcing full dump. Error: $@";
        return 0;
    }

    if ($skip_unchanged) {
        if ($remote_times->{update} == $local_times->{update}) {
            # The "last update" timestamp matches on the local and remote
            # volumes, so the remote volume probably does not need any update
            # at all.
            return undef;
        }
    }

    # The remote volume needs an update. Subtract 3 seconds from the lastupdate
    # time as a "fudge factor", like the normal "vos release" does.
    if ($remote_times->{update} <= 3) {
        return 0;
    }
    return $remote_times->{update} - 3;
}

# Dump the volume associated with the given jobs. We calculate some info about
# the volume, dump it to disk, and report the result to the database.
sub
_do_dump($$$@) {
    my ($server, $prev_state, $lastupdate, @jobs) = @_;
    my $state = 'DUMP_WORK';

    # every single entry from jobs has the same volume name
    my $volname = $jobs[0]->{volname}.".readonly";

    for my $job (@jobs) {
        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $prev_state,
                   to_state => $state,
                   timeout => 300,
                   description => "Checking local volume state");
    }

    my $partition;
    # every single entry from jobs has the same src cell
    ($server, $partition) = find_volume(name => $volname,
                                        type => 'RO',
                                        server => $server,
                                        cell => $jobs[0]->{src_cell});

    # the first argument of _get_size is not used
    my $dump_size = _get_size($jobs[0], $volname, $server, $partition, $lastupdate);
    DEBUG "got dump size $dump_size for volname $volname";

    if (!scratch_ok_jobs($prev_state, $dump_size,
                         config_get('dump/scratch-dir'),
                         config_get('dump/scratch-minfree'),
                         @jobs)) {
        return;
    }

    my $descr = "Starting to dump volume";
    for my $i (0 ,, $#jobs) {
        if ($i == 1) {
            $descr = "Waiting for dump from job $jobs[0]->{jobid}";
        }
        update_job(jobid => $jobs[$i]->{jobid},
                   dvref => \$jobs[$i]->{dv},
                   from_state => $state,
                   vol_lastupdate => $lastupdate,
                   timeout => 120,
                   description => $descr);
        $jobs[$i]->{vol_lastupdate} = $lastupdate;
    }

    my $ids = get_ids(@jobs);
    $ids =~ s/, /_/g;
    my $stderr_fh = File::Temp->new(TEMPLATE => "cccdump_jobs{$ids}_XXXXXX",
                                    TMPDIR => 1, SUFFIX => '.stderr');

    # Determine a filename where we can put our dump blob
    my @dump_fhs;
    for my $job (@jobs) {
        my $dump_fh = File::Temp->new(DIR => config_get('dump/scratch-dir'),
                                      TEMPLATE => "cccdump_job$job->{jobid}_XXXXXX",
                                      SUFFIX => '.dump');
        push(@dump_fhs, $dump_fh);
    }
    $ids =~ s/_/, /g;

    # Start dumping the volume
    my $pid = spawn_child(name => 'vos dump handler',
                          stderr => $stderr_fh->filename,
                          cb => sub {
        my $vos = vos_auth();
        $vos->dump(id => $volname,
                   file => $dump_fhs[0]->filename,
                   server => $server,
                   partition => $partition,
                   time => $jobs[0]->{vol_lastupdate},
                   cell => $jobs[0]->{src_cell})
        or die("vos dump error: ".$vos->errors());
    });
    eval {
        # Wait for dump process to die
        my $last_bytes;
        my $last_time;
        my $pretty_total = pretty_bytes($dump_size);

        monitor_child($pid, { name => 'vos dump handler',
                              error_fh => $stderr_fh,
                              cb_intervals => config_get('dump/monitor-intervals'),
                              cb => sub {
            my ($interval) = @_;

            my ($pretty_bytes, $pretty_rate) = describe_file($dump_fhs[0]->filename,
                                                             \$last_bytes,
                                                             \$last_time);

            $descr = "Running vos dump ($pretty_bytes / $pretty_total dumped, $pretty_rate)";
            for my $i (0 .. $#jobs) {
                if ($i == 1) {
                    $descr = "Dump being performed by job $jobs[0]->{jobid} " .
                             "($pretty_bytes / $pretty_total dumped, $pretty_rate)";
                }
                update_job(jobid => $jobs[$i]->{jobid},
                           dvref => \$jobs[$i]->{dv},
                           from_state => $state,
                           timeout => $interval+60,
                           description => $descr);
            }
        }});
        $pid = undef;
    };
    if ($@) {
        my $error = $@;
        # Kill our child dumping process, so it doesn't hang around
        if (defined($pid)) {
            WARN "Encountered error while dumping for jobs $ids; killing dumping pid $pid";
            kill('INT', $pid);
            $pid = undef;
        }
        die($error);
    }

    for my $job (@jobs) {
        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $state,
                   timeout => 120,
                   description => "Processing finished dump file");
    }

    DEBUG "vos dump successful, processing dump file";
    _dump_success($state, \@jobs, \@dump_fhs);

    INFO "Finished performing dump for jobs $ids (vol '$jobs[0]->{volname}', " .
        "$jobs[0]->{src_cell} -> $jobs[0]->{dst_cell})";
}

# Update the timeout of the jobs received as an argument.
sub
_update_timeout($@) {
    my ($elapsed_time, @jobs) = @_;

    for my $job (@jobs) {
        my $current_timeout = $job->{timeout};
        my $new_timeout = $current_timeout + $elapsed_time;
        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   timeout => $new_timeout);
    }
}

# Find all jobs for the given src/dst cells that need dumps, and perform the
# dumps for them. The dumps are scheduled in child processes using the given
# $pm Parallel::ForkManager object.
sub
process_dumps($$$@) {
    my ($pm, $server, $src_cell, @dst_cells) = @_;
    my $prev_state = 'NEW';
    my $start_state = 'DUMP_START';

    my @jobs;

    # Hashtable used to group the jobs requesting the same volume with
    # the same timestamp
    # vol_table{volume_name}{timestamp} = {job1, job2, ...}
    my %vol_table;

    my $start_time;
    my $current_time;
    my $elapsed_time;

    # Transition all NEW jobs to DUMP_START, and then find all DUMP_START jobs
    @jobs = find_update_jobs(src_cell => $src_cell,
                             dst_cells => \@dst_cells,
                             from_state => $prev_state,
                             to_state => $start_state,
                             timeout => 3600,
                             description => "Waiting for dump to be scheduled");

    # Group the jobs with the same volume_name and timestamp
    $start_time = time();
    for my $job (@jobs) {
        eval {
            my $volname = $job->{volname};
            my $lastupdate = _calc_incremental($job, $start_state);
            if (!defined($lastupdate)) {
                # _calc_incremental said we can skip syncing the volume, so
                # transition the job straight to the final stage
                DEBUG "volume $volname appears to not need a sync";
                update_job(jobid => $job->{jobid},
                           dvref => \$job->{dv},
                           from_state => $start_state,
                           to_state => 'RELEASE_DONE',
                           timeout => 0,
                           description => "Remote volume appears to be up to date;" .
                           "skipping sync");
                next;
            }
            DEBUG "got lastupdate time $lastupdate for volname $volname";
            push(@{$vol_table{$volname}{$lastupdate}}, $job);

            # Refill the timeouts. This loop should not be responsible for
            # expirations
            $current_time = time();
            $elapsed_time = $current_time - $start_time;
            if ($elapsed_time > 300) {
                _update_timeout($elapsed_time, @jobs);
                $start_time = $current_time;
            }
        };
        if ($@) {
            my $error = $@;
            ERROR "Error when getting volume's timestamp for job $job->{jobid}:";
            ERROR $error;
            job_error(jobid => $job->{jobid}, dvref => \$job->{dv});
        }
    }

    for my $volname (sort keys %vol_table) {
        for my $lastupdate (sort keys %{$vol_table{$volname}}) {
            # jobs per volume and timestamp
            my @jbs = @{$vol_table{$volname}{$lastupdate}};
            my $ids = get_ids(@jbs);
            my $pid = $pm->start();
            if ($pid) {
                # In parent
                DEBUG "Spawned child pid $pid to handle dump for jobs ".$ids;
                next;
            }

            # In child
            eval {
                eval {
                     _do_dump($server, $start_state, $lastupdate, @jbs);
                };
                if ($@) {
                    my $error = $@;
                    ERROR "Error when performing dump for jobs $ids:";
                    ERROR $error;
                    for my $job (@jbs) {
                        job_error(jobid => $job->{jobid}, dvref => \$job->{dv});
                    }
                    $pm->finish(1);
                } else {
                    $pm->finish(0);
                }
            };
            # Make sure the child exits, and we don't propagate control back up
            # to our caller.
            exit(1);
        }
    }
}

# Given a bare filename for a dump blob, return the full path to the dump blob
# on disk, suitable for opening.
sub
get_dump_path($) {
    my ($orig_filename) = @_;
    my ($filename, $dirs, undef) = fileparse($orig_filename);
    if (($dirs ne '.') && ($dirs ne '') && ($dirs ne './')) {
        # Make sure the requester cannot just retrieve/unlink any file; just
        # those in our scratch dir
        die("Got dir '$dirs': Directories are not allowed, only bare filenames\n");
    }
    return File::Spec->catfile(config_get('dump/scratch-dir'), $filename);
}

# Given a bare filename for a dump blob, 'cat' the contents of the dump blob to
# stdout.
sub
cat_dump($) {
    my ($orig_filename) = @_;
    my $path = _get_path($orig_filename);

    if (-t STDOUT) {
        die("STDOUT is a tty; refusing to dump file. Pipe through 'cat' to override\n");
    }

    binmode STDOUT;
    copy($path, \*STDOUT)
        or die("Copy failed: $!\n");
}

# Given a bare filename for a dump blob, remove the blob from disk.
sub
remove_dump($) {
    my ($orig_filename) = @_;
    my $path = _get_path($orig_filename);

    unlink($path)
        or die("Cannot remove dump: $!\n");
}

1;
