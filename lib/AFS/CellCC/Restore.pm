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

package AFS::CellCC::Restore;

use strict;
use warnings;

use 5.008_000;

use Digest;
use File::Basename;
use File::stat;
use File::Temp;

# This turns on the DEBUG, INFO, WARN, ERROR functions
use Log::Log4perl qw(:easy);

use AFS::CellCC::Config qw(config_get);
use AFS::CellCC::Const qw($VERSION_STRING);
use AFS::CellCC::Remoteclient qw(remctl_cmd);
use AFS::CellCC::VOS qw(vos_auth find_volume volume_exists check_volume_sites
                        volume_all_sites);
use AFS::CellCC::Util qw(spawn_child monitor_child describe_file pretty_bytes scratch_ok
                         calc_checksum);
use AFS::CellCC::DB qw(db_rw
                       update_job
                       find_update_jobs
                       job_error);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(process_restores);

# Fetch a dump blob for the given $job from the remote machine where it
# resides, using the given @$remote_cellcc command.
sub
_fetch_dump($$$$) {
    my ($job, $work_state, $remote_cellcc, $stderr_fh) = @_;
    my $scratchdir = config_get('restore/scratch-dir');

    my $dump_fh = File::Temp->new(DIR => $scratchdir,
                                  TEMPLATE => "cccrestore_job$job->{jobid}_XXXXXX",
                                  SUFFIX => '.dump');

    my $pid = spawn_child(stdout => $dump_fh->filename,
                          stderr => $stderr_fh->filename,
                          name => 'remctl get-dump',
                          exec => [@$remote_cellcc, 'get-dump', $job->{dump_filename}]);

    my ($last_bytes, $last_time);
    monitor_child($pid, { name => 'remctl get-dump',
                          error_fh => $stderr_fh,
                          cb_intervals => config_get('xfer/monitor-intervals'),
                          cb => sub {
        my ($interval) = @_;
        my ($pretty_size, $pretty_rate) = describe_file($dump_fh->filename,
                                                        \$last_bytes, \$last_time);
        if (defined($job->{dump_filesize})) {
            $pretty_size .= " / ".pretty_bytes($job->{dump_filesize});
        }

        my $descr = "Transferring volume dump ($pretty_size transferred, $pretty_rate)";

        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $work_state,
                   timeout => $interval+60,
                   description => $descr);
    }});

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               restore_filename => basename($dump_fh->filename),
               timeout => 60,
               description => "Dump transferred, about to check received dump");

    $dump_fh->unlink_on_destroy(0);
    $job->{restore_filename} = basename($dump_fh->filename);
}

# Check if the checksum for the dump blob in $path matches the checksum
# recorded in job $job.
sub
_checksum_valid($$$) {
    my ($job, $path, $state) = @_;
    my $fh;

    my $jobid = $job->{jobid};
    my $dvref = \$job->{dv};

    my ($algo, undef) = split(/:/, $job->{dump_checksum});

    if (!open($fh, '<', $path)) {
        WARN "Cannot open dump file $path: $!\n";
        return 0;
    }

    my $sb = stat($fh);
    if ($sb->size != $job->{dump_filesize}) {
        WARN "Dump file ($path) has wrong size: ".$sb->size." != ".$job->{dump_filesize}."\n";
        return 0;
    }

    DEBUG "filesize valid (path $path): ".$sb->size;

    my $checksum = calc_checksum($fh, $sb->size, $algo, $jobid, $dvref, $state);
    close($fh);

    if ($checksum ne $job->{dump_checksum}) {
        WARN "Dump file ($path) checksum mismatch: $checksum != ".$job->{dump_checksum}."\n";
        return 0;
    }

    DEBUG "checksum valid (path $path): $checksum";

    return 1;
}

# Remove the dump blob on the remote dump-server for job $job, using the given
# command @$remote_cellcc.
sub
_remove_dump($$$$) {
    my ($job, $state, $stderr_fh, $remote_cellcc) = @_;
    my $pid;

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $state,
               timeout => 60,
               description => "Removing dump file on dump host");

    truncate($stderr_fh, 0);

    $pid = spawn_child(stderr => $stderr_fh->filename,
                       name => 'remctl remove-dump',
                       exec => [@$remote_cellcc, 'remove-dump', $job->{dump_filename}]);

    monitor_child($pid, { name => 'remctl remove-dump',
                          error_fh => $stderr_fh, });

    # We removed the file on the dump host successfully; tell the db that we
    # did so.
    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               timeout => 60,
               dump_filename => undef);
    $job->{dump_filename} = undef;
}

# Get the remote cellcc remctl command to run in order to contact the
# dump-server associated with the given job.
sub
_remote_cellcc_command($) {
    my ($job) = @_;
    my $fqdn = $job->{dump_fqdn};
    my @remctl_args;

    if (defined($job->{dump_port})) {
        push(@remctl_args, '-p', $job->{dump_port});
    }

    my $service = config_get('remctl/service');
    if (defined($service)) {
        $service =~ s/<FQDN>/$fqdn/g;
        if ($service =~ m/[<>]/) {
            # In case someone typoes FQDN, or tries to use some other macro name,
            # error out instead of passing something like '-s host/<FDQN>'.
            die("Config error: remctl/service directive looks weird after ".
                "subtitution: $service");
        }

        push(@remctl_args, '-s', $service);
    }

    return (config_get('k5start/command'), '-q',
            '-f', config_get('remctl/client-keytab'),
            config_get('remctl/princ'),
            '--',
            config_get('remctl/command'),
            @remctl_args, $fqdn, 'cellcc', 'remctl');
}

# Transfer the dump blob for $job from the associated dump-server to local
# disk. If all is successful, we remove the blob from the remote dump-server as
# well.
sub
_do_xfer($$) {
    my ($job, $start_state) = @_;
    my $work_state = 'XFER_WORK';
    my $done_state = 'XFER_DONE';

    if (!defined($job->{dump_method})) {
        die("DB Error: job $job->{jobid} is in an XFER state, but dump_method is undefined\n");
    }

    if ($job->{dump_method} ne 'remctl') {
        die("dump for job $job->{jobid} advertises unsupported method '".$job->{dump_method}."'");
    }

    # Check the checksum algorithm early; this should bail out if the
    # checksum algorithm is unknown/not supported.
    my ($algo, undef) = split(/:/, $job->{dump_checksum});
    my $digest = Digest->new($algo);

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $start_state,
               to_state => $work_state,
               timeout => 120,
               description => "Contacting ".$job->{dump_fqdn}." to get volume blob");

    my $stderr_fh = File::Temp->new(TEMPLATE => "cccxfer_job$job->{jobid}_XXXXXX",
                                    TMPDIR => 1, SUFFIX => '.stderr');

    my @remote_cellcc = remctl_cmd(fqdn => $job->{dump_fqdn},
                                   port => $job->{dump_port});

    # First, we have to get the actual file from the dump host to the restore
    # host, unless the db says we already have it on the restore host.
    if (!$job->{restore_filename}) {
        if (!scratch_ok($start_state, $job->{dump_filesize},
                        config_get('restore/scratch-dir'),
                        config_get('restore/scratch-minfree'), $job)) {
            return;
        }

        _fetch_dump($job, $work_state, \@remote_cellcc, $stderr_fh);
    }

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               timeout => 120,
               description => "Checking retrieved dump file");

    # See if the retrieved dump matches the checksum in the database
    my $dump_path = File::Spec->catfile(config_get('restore/scratch-dir'), $job->{restore_filename});
    if (!_checksum_valid($job, $dump_path, $work_state)) {
        unlink($dump_path);
        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   restore_filename => undef,
                   timeout => 60);
        $job->{restore_filename} = undef;
        die("Fetched dump file ($dump_path) is invalid\n");
    }

    if ($job->{dump_filename}) {
        _remove_dump($job, $work_state, $stderr_fh, \@remote_cellcc);
    }

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               to_state => $done_state,
               timeout => undef,
               description => "Retrieved dump file, waiting to restore to local cell");
}

# Create the volume associated with the given $job in the destination cell.
# This should be called only if the volume doesn't exist in the destination
# cell.
sub
_create_volume($$) {
    my ($job, $state) = @_;

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $state,
               timeout => 1200,
               description => "Creating volume");

    my $command = "CELLCC_PS_VOLUME='$job->{volname}' ".
                  "CELLCC_PS_CELL='$job->{dst_cell}' ".
                  "CELLCC_PS_DST_CELL='$job->{dst_cell}' ".
                  "CELLCC_PS_SRC_CELL='$job->{src_cell}' ".
                  config_get('pick-sites/command');

    DEBUG "Running pick-sites command: $command";

    open(my $ph, '-|', $command)
        or die("Cannot run pick-sites: $!\n");

    my @sites;

    while (<$ph>) {
        chomp;
        if (m/^#/ or m/^$/) {
            # Skip "comment" or blank lines
            next;
        }
        my $line = $_;
        my @parts = split(" ", $line);

        if (scalar(@parts) != 2) {
            die("Error parsing pick-sites line: $line\n");
        }
        push(@sites, {server => $parts[0], partition => $parts[1]});
    }

    close($ph) or die("pick-sites: Error running $command: errno ".($!+0)." exit status $?\n");

    if (scalar(@sites) < 1) {
        die("pick-sites gave us too few sites (need at least 1)\n");
    }

    my $vos = vos_auth();

    my $rwserver = $sites[0]->{server};
    my $rwpartition = $sites[0]->{partition};

    $vos->create(server => $rwserver,
                 partition => $rwpartition,
                 name => $job->{volname},
                 maxquota => 1,
                 cell => $job->{dst_cell})
    or die("vos create error: ".$vos->errors());

    for my $site (@sites) {
        $vos->addsite(id => $job->{volname},
                      server => $site->{server},
                      partition => $site->{partition},
                      cell => $job->{dst_cell})
            or die("vos addsite error: ".$vos->errors());
    }

    if (@sites) {
        $vos->release(id => $job->{volname}, cell => $job->{dst_cell})
            or die("vos release error: ".$vos->errors());
    }

    $vos->offline(id => $job->{volname},
                  server => $rwserver,
                  partition => $rwpartition,
                  cell => $job->{dst_cell})
    or die("vos offline error: ".$vos->errors());


    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $state,
               timeout => 1200,
               description => "Volume created, checking volume");
}

# Do what is needed after a volume has been successfully restored. Currently
# that means we just remove our dump blob on local disk, and update the
# database to indicate as such. 
sub
_restore_success($) {
    my ($job) = @_;

    my $filename = $job->{restore_filename};
    if ($filename) {
        my $path = File::Spec->catfile(config_get('restore/scratch-dir'),
                                       $filename);
        unlink($path) or WARN "Cannot unlink dump file $path";

        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   timeout => 60,
                   restore_filename => undef);
        $job->{restore_filename} = undef;
    }
}

# Restore the dump blob for the volume associated with $job in its destination
# cell, creating the volume first if necessary. The dump blob should already
# have been transferred to local disk from the remote dump-server first.
sub
_do_restore($$) {
    my ($job, $start_state) = @_;
    my $work_state = 'RESTORE_WORK';
    my $done_state = 'RESTORE_DONE';

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $start_state,
               to_state => $work_state,
               timeout => 1200,
               description => "Checking local volume state");

    if (!volume_exists($job->{volname}, $job->{dst_cell})) {
        _create_volume($job, $work_state);
    }

    my ($server, $partition) = find_volume(name => $job->{volname},
                                           type => 'RW',
                                           cell => $job->{dst_cell});

    my $dump_path = File::Spec->catfile(config_get('restore/scratch-dir'),
                                        $job->{restore_filename});
    if (!-r $dump_path) {
        die("Cannot read dump file $dump_path\n");
    }

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               timeout => 120,
               description => "Starting to restore volume");

    my $stderr_fh = File::Temp->new(TEMPLATE => "cccrestore_job$job->{jobid}_XXXXXX",
                                    TMPDIR => 1, SUFFIX => '.stderr');

    my $pid = spawn_child(name => 'vos restore handler',
                          stderr => $stderr_fh->filename,
                          cb => sub {
        my $vos = vos_auth();
        $vos->restore(name => $job->{volname},
                      server => $server,
                      partition => $partition,
                      overwrite => 'incremental',
                      lastupdate => 'dump',
                      cell => $job->{dst_cell},
                      file => $dump_path,
        ) or die("vos restore error: ".$vos->errors());
    });

    monitor_child($pid, { name => 'vos restore handler',
                          error_fh => $stderr_fh,
                          cb_intervals => config_get('restore/monitor-intervals'),
                          cb => sub {
        my ($interval) = @_;

        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $work_state,
                   timeout => $interval + 60,
                   description => "Running vos restore");
    }});

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               timeout => 60,
               description => "vos restore done, cleaning up dump");

    _restore_success($job);

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               to_state => $done_state,
               timeout => undef,
               description => "Done restoring, waiting to release");
}

# Release the volume for $job in the destination cell (this should be done
# right after the volume has been restored).
sub
_do_release($$) {
    my ($job, $start_state) = @_;
    my $work_state = 'RELEASE_WORK';
    my $done_state = 'RELEASE_DONE';

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $start_state,
               to_state => $work_state,
               timeout => 120,
               description => "Starting volume release");

    my $stderr_fh = File::Temp->new(TEMPLATE => "cccrelease_job$job->{jobid}_XXXXXX",
                                    TMPDIR => 1, SUFFIX => '.stderr');

    my $pid = spawn_child(name => 'vos release handler',
                          stderr => $stderr_fh->filename,
                          cb => sub {
        my $vos = vos_auth();
        $vos->release(id => $job->{volname},
                      cell => $job->{dst_cell})
        or die("vos release error: ".$vos->errors());
    });

    monitor_child($pid, { name => 'vos release handler',
                          error_fh => $stderr_fh,
                          cb_intervals => config_get('release/monitor-intervals'),
                          cb => sub {
        my ($interval) = @_;
        update_job(jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $work_state,
                   timeout => $interval + 60,
                   description => "Running vos release");
    }});

    # After the release is done, check that the sites all look okay; no "old
    # site"/"new site" stuff or locked vlentries, etc.
    check_volume_sites($job->{volname}, $job->{dst_cell});

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               to_state => $done_state,
               timeout => undef,
               description => "Release done");

    INFO "Finished releasing volume for job $job->{jobid} (vol '$job->{volname}', $job->{src_cell} -> $job->{dst_cell})";
}

# Delete the volume for $job in the destination cell. We delete all sites for
# the volume, as well as the RW.
sub
_do_delete($$) {
    my ($job, $start_state) = @_;
    my $work_state = 'DELETE_DEST_WORK';
    my $done_state = 'DELETE_DEST_DONE';

    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $start_state,
               to_state => $work_state,
               timeout => 1200,
               description => "Starting volume delete");

    if (volume_exists($job->{volname}, $job->{dst_cell})) {
        my @sites = volume_all_sites($job->{volname}, $job->{dst_cell});

        # Delete all RO sites first, then BK, then RW.
        my $rank = sub {
            my ($site) = @_;
            if ($site->{type} eq 'RO') {
                return 0;
            } elsif ($site->{type} eq 'BK') {
                return 1;
            } elsif ($site->{type} eq 'RW') {
                return 2;
            } else {
                die("Unknown type $site->{type} for volume $job->{volname}");
            }
        };

        @sites = sort {
            my $rank_a = $rank->($a);
            my $rank_b = $rank->($b);
            return $rank_a <=> $rank_b;
        } @sites;

        my $vos = vos_auth();

        for my $site (@sites) {
            update_job(jobid => $job->{jobid},
                       dvref => \$job->{dv},
                       from_state => $work_state,
                       timeout => 1200,
                       description => "Deleting volume $site->{name} from ".
                                      "$site->{server} $site->{partition}");

            $vos->remove(server => $site->{server},
                         partition => $site->{partition},
                         id => $site->{name},
                         cell => $job->{dst_cell})
                or die("vos remove error: ".$vos->errors());
        }

    } else {
        DEBUG "Not deleting volume $job->{volname} cell $job->{dst_cell} ".
              "jobid $job->{jobid}, because it doesn't seem to exist.";
    }


    update_job(jobid => $job->{jobid},
               dvref => \$job->{dv},
               from_state => $work_state,
               to_state => $done_state,
               timeout => undef,
               description => "Volume delete done");
}

# Find all jobs for the given destination cell $dst_cell that are in
# $prev_state or $start_state, and perform the relevant work on that job. The
# jobs are processed in child processes managed by the Parallel::ForkManager
# $pm.
sub
_process_jobs($$$$$&) {
    my ($pm, $queue, $dst_cell, $prev_state, $start_state, $worker) = @_;

    my @jobs;

    # Transition all $prev_state jobs to $start_state, and then find all
    # $start_state jobs
    @jobs = find_update_jobs(dst_cell => $dst_cell,
                             qname => $queue,
                             from_state => $prev_state,
                             to_state => $start_state,
                             timeout => 3600,
                             description => "Waiting for next stage to be scheduled");

    # Now spawn children, and have them do the actual work
    for my $job (@jobs) {
        my $pid = $pm->start();
        if ($pid) {
            # In parent
            DEBUG "Spawned child pid $pid to handle job ".$job->{jobid}." in state $start_state";
            next;
        }

        # In child
        eval {
            eval {
                $worker->($job, $start_state);
            };
            if ($@) {
                my $error = $@;
                ERROR "Error when processing restore for job $job->{jobid}:";
                ERROR $error;
                job_error(jobid => $job->{jobid}, dvref => \$job->{dv});
                $pm->finish(1);

            } else {
                DEBUG "Finished $start_state for job $job->{jobid}";
                $pm->finish(0);
            }
        };
        # Make sure the child exits, and we don't propagate control back up to
        # our caller.
        exit(1);
    }
}

# Do work on any jobs we can find that need transferring, restoring, releasing,
# or deleting.
sub
_process_restores($$$) {
    my ($pm, $queue, $dst_cell) = @_;
    my @jobs;

    _process_jobs($pm, $queue, $dst_cell, 'DUMP_DONE', 'XFER_START', \&_do_xfer);
    _process_jobs($pm, $queue, $dst_cell, 'XFER_DONE', 'RESTORE_START', \&_do_restore);
    _process_jobs($pm, $queue, $dst_cell, 'RESTORE_DONE', 'RELEASE_START', \&_do_release);

    _process_jobs($pm, $queue, $dst_cell, 'DELETE_NEW', 'DELETE_DEST_START', \&_do_delete);
}

# Run the restore-server processing for the given $queue and cell $dst_cell. If
# $opts->{once} is set, we just scan for jobs with work to do once, and then
# exit after doing the work. Otherwise, we periodically sleep and re-check for
# work to do.
sub
server($$;$) {
    my ($queue, $dst_cell, $opts) = @_;
    my $once = 0;

    if (!defined($opts)) {
        $opts = {};
    }

    if ($opts->{once}) {
        $once = 1;
    }

    my $maxp = config_get("restore/queues/$queue/max-parallel");
    my $pm = Parallel::ForkManager->new($maxp);

    my $term_handler = 'DEFAULT';

    if (!$once) {
        INFO "CellCC restore-server $VERSION_STRING starting up";
        $term_handler = sub {
            INFO "shutting down";
            exit(0);
        };
    }

    INFO "Restoring up to $maxp volume(s) in parallel for queue '$queue'";

    local $SIG{INT} = $term_handler;
    local $SIG{TERM} = $term_handler;

    while (1) {
        eval {
            AFS::CellCC::Restore::_process_restores($pm, $queue, $dst_cell);
        };
        if ($@) {
            my $error = $@;
            if ($once) {
                die($error);
            }
            ERROR $error;
        }

        if ($once) {
            last;
        }

        my $seconds = config_get('restore/check-interval');
        sleep($seconds);
    }

    $pm->wait_all_children();
}
