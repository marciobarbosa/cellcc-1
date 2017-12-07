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

package AFS::CellCC::DB;

use strict;
use warnings;

use Carp;
use DBIx::Simple;
use DateTime::Format::MySQL;
use Log::Log4perl qw(:easy);

use AFS::CellCC::Config qw(config_get);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(db_rw
                    db_ro
                    connect_rw
                    connect_ro
                    create_job
                    find_jobs
                    update_job
                    archive_job
                    kill_job
                    find_update_jobs
                    describe_jobs
                    describe_dummy_jobs
                    job_error
                    job_reset
                    jobinfo_stringify
                    retry_job
                    update_jobs
);

our $DB_VERSION = 1;

my $_first_connect = 1;

# Helper sub for connect_rw/connect_ro
sub
_connect($$$$) {
    my ($dsn, $user, $pass, $opts) = @_;
    my $dbh = DBIx::Simple->new($dsn, $user, $pass,
                                { %$opts, RaiseError => 1, PrintError => 0 });
    $dbh->keep_statements = 0;
    if ($_first_connect) {
        my @rows = $dbh->query('SELECT version FROM versions WHERE version = ?',
                               $DB_VERSION)->arrays();
        if (@rows != 1) {
            die("DB Error: Database appears to be incompatible version (does not support version $DB_VERSION)\n");
        }
        $_first_connect = 0;
    }
    return $dbh;
}

# Connect to the db with a read/write connection
sub
connect_rw() {
    return _connect(config_get('db/rw/dsn'),
                    config_get('db/rw/user'),
                    config_get('db/rw/pass'),
                    { AutoCommit => 0});
}

# Connect to the db with a readonly connection
sub
connect_ro() {
    return _connect(config_get('db/ro/dsn'),
                    config_get('db/ro/user'),
                    config_get('db/ro/pass'),
                    { ReadOnly => 1 });
}

# Run the given code with a read/write db connection. Use like so:
# db_rw(sub($) {
#     my ($dbh) = @_;
#     update_db_foo($dbh);
# });
sub
db_rw(&) {
    my ($sub) = @_;
    my $dbh = connect_rw();
    eval {
        $sub->($dbh)
    };
    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        $dbh->disconnect();
        die($error);
    }
    $dbh->commit();
    $dbh->disconnect();
}

# Run the given code with a readonly db connection. Use like so:
# db_ro(sub($) {
#     my ($dbh) = @_;
#     fetch_from_db_foo($dbh);
# });
sub
db_ro(&) {
    my ($sub) = @_;
    my $dbh = connect_ro();
    eval {
        $sub->($dbh);
    };
    my $error = $@;
    $dbh->disconnect();
    if ($error) {
        die($error);
    }
}

# We have subs with a lot of keyword args; this just checks for errors in
# missing arguments, or mispelled ones, etc
sub
_check_args($%) {
    my ($argref, %spec) = @_;

    my %args = %$argref;
    my @reqs;
    my @opts;
    my %ret;

    if (defined($spec{req})) {
        @reqs = @{$spec{req}};
    }
    if (defined($spec{opt})) {
        @opts = @{$spec{opt}};
    }

    for my $var (@reqs) {
        if (!exists($args{$var})) {
            confess("Internal error: missing arg '$var'\n");
        }
    }
    for my $var (@reqs, @opts) {
        if (exists($args{$var})) {
            $ret{$var} = $args{$var};
            delete $args{$var};
        }
    }

    for my $var (keys %args) {
        confess("Internal error: extra arg '$var'\n");
    }

    return %ret;
}

# For our pseudo-SQL::Abstract stuff, we allow raw SQL to be embedded with
# arrayrefs like passing ['CURRENT_TIMESTAMP']. We just make sure the arrayref
# only contains one element (since we only support using one element), and it
# contains a literal string, not a ref to something else, etc.
#
# This accepts the arrayref as an arg (e.g. ['CURRENT_TIMESTAMP']), and returns
# the raw SQL (e.g. "CURRENT_TIMESTAMP").
sub
_check_sql_arrayref($) {
    my ($val) = @_;
    if (@$val != 1) {
        # We only support 1-length arrays for raw sql
        confess("Internal error: sql arrayref has ".@$val." elements");
    }
    $val = $val->[0];
    if (ref($val) ne '') {
        confess("Internal error: sql arrayref contains '".ref($val)."' ref");
    }
    return $val;
}

# Wrapper around $dbh->query('INSERT') just to verify we can get the inserted
# row id back. Ideally we would use SQL::Abstract here or something similar,
# it requires quite a bit of dependencies, and it doesn't exist in EPEL for
# EL7.
sub
_insert($$$) {
    my ($dbh, $table, $data) = @_;
    my @cols = sort keys %$data;
    my @bind_vals;
    my @sql_vals;

    for my $col (@cols) {
        my $val = $data->{$col};
        # If value is an arrayref, assume it is raw sql to insert
        if (ref($val) eq 'ARRAY') {
            $val = _check_sql_arrayref($val);
            push(@sql_vals, $val);
        } else {
            push(@sql_vals, '?');
            push(@bind_vals, $val);
        }
    }

    my $colstr = join(',', @cols);
    my $valstr = join(',', @sql_vals);

    my $sql = "INSERT INTO $table ($colstr) VALUES ($valstr)";
    DEBUG "db _insert sql '$sql' bind values ".join(',', map { defined($_) ? $_ : "undef" } @bind_vals);
    $dbh->query($sql, @bind_vals);

    my $rowid = $dbh->last_insert_id(undef, undef, undef, undef);
    if (!defined($rowid)) {
        die("DB Error: cannot determine inserted row id for '$table'\n");
    }
    return $rowid;
}

# Wrapper around $dbh->query('UPDATE'). See _insert().
sub
_update($$$$) {
    my ($dbh, $table, $data, $where) = @_;
    my @data_cols = sort keys %$data;
    my @where_cols = sort keys %$where;

    my @set_clauses;
    my @where_clauses;
    my @bind_vals;

    for my $col (@data_cols) {
        my $val = $data->{$col};
        if (ref($val) eq 'ARRAY') {
            $val = _check_sql_arrayref($val);
            push(@set_clauses, "$col = $val");
        } else {
            push(@set_clauses, "$col = ?");
            push(@bind_vals, $val);
        }
    }

    for my $col (@where_cols) {
        my $val = $where->{$col};
        if (ref($val) eq 'ARRAY') {
            $val = _check_sql_arrayref($val);
            push(@where_clauses, "$col = $val");
        } else {
            push(@where_clauses, "$col = ?");
            push(@bind_vals, $val);
        }
    }

    my $setstr = join(', ', @set_clauses);
    my $wherestr = join(' AND ', @where_clauses);

    my $sql = "UPDATE $table SET $setstr WHERE $wherestr";
    DEBUG "db _update sql '$sql' bind values ".join(',', map { defined($_) ? $_ : "undef" } @bind_vals);
    return $dbh->query($sql, @bind_vals);
}

# Create a new job in the db. Returns the jobid for the created job.
sub
create_job(%) {
    my %info = _check_args({@_},
        req => [qw(dbh
                   src_cell
                   dst_cell
                   volname
                   qname
                   state
                   description
        )],
    );
    my $dbh = $info{dbh};

    return _insert($dbh, 'jobs', {
        src_cell    => $info{src_cell},
        dst_cell    => $info{dst_cell},
        qname       => $info{qname},
        volname     => $info{volname},
        dv          => 1,
        state       => $info{state},
        timeout     => undef,
        description => $info{description},
        ctime       => ['CURRENT_TIMESTAMP'],
        mtime       => ['CURRENT_TIMESTAMP'],
        status_fqdn => config_get('fqdn'),
    });
}

# Find running jobs in the db matching the given criteria. Returns an array of
# hashrefs; each hashref contains information about a matched job.
sub
find_jobs(%) {
    my %info = _check_args({@_},
        req => [qw(dbh
                   dst_cell
                   state
        )],
        opt => [qw(src_cell
                   qname
                   jobid
        )],
    );

    my $dbh = $info{dbh};

    my $sql = <<'END';
    SELECT id AS jobid,
           src_cell,
           dst_cell,
           dv,
           volname,
           vol_lastupdate,
           qname,
           dump_fqdn,
           dump_method,
           dump_port,
           dump_filename,
           restore_filename,
           dump_checksum,
           dump_filesize
    FROM jobs
    WHERE
         (? IS NULL OR id = ?)
         AND (? IS NULL OR qname = ?)
         AND (? IS NULL OR src_cell = ?)
         AND dst_cell = ?
         AND state = ?
    ORDER BY mtime ASC;
END
    my $res = $dbh->query($sql,
                          $info{jobid}, $info{jobid},
                          $info{qname}, $info{qname},
                          $info{src_cell}, $info{src_cell},
                          $info{dst_cell},
                          $info{state});

    my @jobs = $res->hashes();

    DEBUG "found ".@jobs." jobs in state ".$info{state}." (".
          (defined($info{qname}) ? 'queue '.$info{qname}
                                 : 'any queue').
          ', '.
          (defined($info{src_cell}) ? "cell ".$info{src_cell}
                                    : "any cell").
          " -> ".$info{dst_cell}.")";

    return @jobs;
}

# Change some information about a job. This always updates the mtime for the
# job and increments the dv. The job's dv must match the given dvref before we
# change anything; the new dv is given back in dvref.
sub
_job_set(%) {
    my %info = _check_args({@_},
        req => [qw(jobid
                   dvref
                   timeout
        )],
        opt => [qw(dbh
                   from_state
                   to_state
                   description
                   errors
                   last_good_state
                   dump_filename
                   dump_fqdn
                   dump_method
                   dump_port
                   dump_filename
                   dump_checksum
                   dump_filesize
                   restore_filename
                   vol_lastupdate
                   errorlimit_mtime
        )],
    );

    my $dbh = $info{dbh};
    if (!$dbh) {
        db_rw(sub($) {
            my ($sub_dbh) = @_;
            _job_set(%info, dbh => $sub_dbh);
        });
        return;
    }

    my $dv = ${$info{dvref}};
    my $new_dv = $dv + 1;

    my %where = (id => $info{jobid}, dv => $dv);
    my %data = (dv => $new_dv,
                timeout => $info{timeout},
                status_fqdn => config_get('fqdn'),
                mtime => ['CURRENT_TIMESTAMP']);

    if ($info{from_state}) {
        $where{state} = $info{from_state};
    }

    for my $col (qw(description
                    last_good_state
                    errors
                    dump_filename
                    dump_fqdn
                    dump_method
                    dump_port
                    dump_filename
                    dump_checksum
                    dump_filesize
                    restore_filename
                    vol_lastupdate)) {
        if (exists $info{$col}) {
            $data{$col} = $info{$col};
        }
    }
    if (exists $info{to_state}) {
        $data{state} = $info{to_state};
    }
    if (exists $info{errorlimit_mtime}) {
        if (defined($info{errorlimit_mtime})) {
            # Always update errorlimit_mtime to just the current timestamp. We
            # don't need to bother with putting a time in the proper format or
            # anything; we only ever need to set the time to 'now'.
            $data{errorlimit_mtime} = ['CURRENT_TIMESTAMP'];

        } else {
            # ...but if we're told to clear errorlimit_mtime, then clear it.
            $data{errorlimit_mtime} = undef;
        }
    }

    my $res = _update($dbh, 'jobs', \%data, \%where);
    if ($res->rows() != 1) {
        confess("DB Error: Updated ".$res->rows()." rows when trying to update job $info{jobid}\n");
    }

    ${$info{dvref}} = $new_dv;
}

# High-level function to change information about a job. Similar to _job_set,
# but called from other modules.
sub
update_job(%) {
    my %info = _check_args({@_},
        req => [qw(jobid
                   dvref
                   timeout
        )],
        opt => [qw(dbh
                   from_state
                   to_state
                   dump_filename
                   dump_fqdn
                   dump_method
                   dump_port
                   dump_filename
                   dump_checksum
                   dump_filesize
                   restore_filename
                   description
                   vol_lastupdate
                   errorlimit_mtime
        )],
    );
    my $dv = ${$info{dvref}};
    my $statestr = '';
    if (exists $info{from_state}) {
        $statestr .= " state $info{from_state}";
    }
    if (exists $info{to_state}) {
        $statestr .= " -> state $info{to_state}";
    }

    DEBUG "Update job $info{jobid} dv $dv$statestr";

    _job_set(%info);
}

# Similar to update_job but updates several jobs at once.
sub
update_jobs($%) {
    my ($jobsref, %args) = @_;
    my @jobs = @{$jobsref};
    my %info = _check_args(\%args,
        req => [qw(timeout
        )],
        opt => [qw(dbh
                   from_state
                   to_state
                   dump_filename
                   dump_fqdn
                   dump_method
                   dump_port
                   dump_filename
                   dump_checksum
                   dump_filesize
                   restore_filename
                   description
                   vol_lastupdate
                   errorlimit_mtime
        )],
    );
    my $dbh = $info{dbh};
    if (!$dbh) {
        db_rw(sub($) {
            my ($sub_dbh) = @_;
            $info{dbh} = $sub_dbh;
            update_jobs($jobsref, %info);
        });
        return;
    }
    my @dvs;
    my @ids;
    for my $job (@jobs) {
        push(@dvs, $job->{dv});
        push(@ids, $job->{jobid});
    }

    my $statestr = '';
    if (exists $info{from_state}) {
        $statestr .= " state $info{from_state}";
    }
    if (exists $info{to_state}) {
        $statestr .= " -> state $info{to_state}";
    }

    DEBUG "Update jobs ". join( ',', @ids ) ." dvs ". join( ',', @dvs ) ."$statestr";

    for my $job (@jobs) {
        $info{jobid} = $job->{jobid};
        $info{dvref} = \$job->{dv};

        _job_set(%info);
    }
}

# Moves all jobs in 'from_state' to 'to_state', and then returns all jobs in
# 'to_state'.
sub
find_update_jobs(%) {
    my %info = _check_args({@_},
        req => [qw(from_state
                   to_state
                   description
        )],
        opt => [qw(dbh
                   src_cell
                   dst_cell
                   dst_cells
                   qname
                   timeout
        )],
    );
    my $dbh = $info{dbh};

    if (!$dbh) {
        my @jobs;
        db_rw(sub($) {
            my ($sub_dbh) = @_;
            @jobs = find_update_jobs(%info, dbh => $sub_dbh);
        });
        return @jobs;
    }

    my @dst_cells;
    if (defined($info{dst_cell})) {
        @dst_cells = ($info{dst_cell});
    } elsif (defined($info{dst_cells})) {
        @dst_cells = @{ $info{dst_cells} };
    }

    if (scalar(@dst_cells) < 1) {
        confess("Internal error: find_update_jobs need at least one dst cell");
    }

    my @jobs;

    # First, find all the jobs in our 'from state', and transition them to our
    # 'to state'
    for my $dst_cell (@dst_cells) {
        push(@jobs, find_jobs(dbh => $dbh,
                              qname => $info{qname},
                              src_cell => $info{src_cell},
                              dst_cell => $dst_cell,
                              state => $info{from_state}));
    }

    for my $job (@jobs) {
        update_job(dbh => $dbh,
                   jobid => $job->{jobid},
                   dvref => \$job->{dv},
                   from_state => $info{from_state},
                   to_state => $info{to_state},
                   timeout => $info{timeout},
                   description => $info{description});
    }

    # Now find all jobs in our 'to state'. There may be more jobs than we just
    # transitioned above, since someone could have reset a job back to this
    # state, or something similar.
    @jobs = ();
    for my $dst_cell (@dst_cells) {
        push(@jobs, find_jobs(dbh => $dbh,
                              qname => $info{qname},
                              src_cell => $info{src_cell},
                              dst_cell => $dst_cell,
                              state => $info{to_state}));
    }


    return @jobs;
}

# Parse a timestamp from sql results into a DateTime object. Returns the
# DateTime object if successful.
sub
_parse_dt($$) {
    my ($dbh, $str) = @_;
    my $driver = lc $dbh->dbh->{Driver}->{Name};
    if ($driver eq 'mysql') {
        return DateTime::Format::MySQL->parse_datetime($str);
    }
    die("We do not support database driver '$driver'\n");
}

# This returns information about some fictional "dummy" jobs. This can be
# useful for providing example output for something that describes jobs; so we
# don't need to find actual jobs to describe. This can also be a helpful
# reference to see what job hashrefs are expected to look like.
sub
describe_dummy_jobs($) {
    my ($count) = @_;
    my @jobs;
    my $jobid = 4;
    for (1..$count) {
        push(@jobs, {
            jobid => $jobid,
            dv => 4,
            errors => 0,
            last_good_state => "RELEASE_START",
            src_cell => "source.example.com",
            dst_cell => "destination.example.com",
            volname => "example.volume",
            vol_lastupdate => 0,
            qname => "default",
            state => "ERROR",
            dump_fqdn => "dumphost.example.com",
            dump_method => "remctl",
            dump_port => 4373,
            dump_filename => "cccdump_job${jobid}_123456",
            restore_filename => "cccrestore_job${jobid}_123456",
            dump_checksum => "MD5:d41d8cd98f00b204e9800998ecf8427e",
            dump_filesize => 0,
            ctime => DateTime->now(),
            mtime => DateTime->now(),
            errorlimit_mtime => DateTime->now(),
            now_server => DateTime->now(),
            status_fqdn => 'stathost.example.com',
            timeout => 60,
            description => "Test job description; not a real job",
        });
        $jobid++;
    }
    return \@jobs;
}

# Find jobs matching the given search criteria. This is similar to find_jobs(),
# but returns more information about the jobs; the results of this are intended
# to be used by human-readable reporting tools.
sub
describe_jobs(%) {
    my %info = _check_args({@_},
        opt => [qw(dbh
                   src_cell
                   dst_cell
                   volname
                   state
                   jobid
        )],
    );

    my $dbh = $info{dbh};
    if (!$dbh) {
        my @ret;
        db_ro(sub($) {
            my ($sub_dbh) = @_;
            @ret = describe_jobs(%info, dbh => $sub_dbh);
        });
        return @ret;
    }

    my $sql = <<'END';
    SELECT id AS jobid,
           dv,
           errors,
           last_good_state,
           src_cell,
           dst_cell,
           volname,
           vol_lastupdate,
           qname,
           state,
           dump_fqdn,
           dump_method,
           dump_port,
           dump_filename,
           restore_filename,
           dump_checksum,
           dump_filesize,
           ctime,
           mtime,
           errorlimit_mtime,
           CURRENT_TIMESTAMP AS now_server,
           status_fqdn,
           timeout,
           description
    FROM
        jobs
    WHERE
        (? IS NULL OR id = ?)
        AND (? IS NULL OR src_cell = ?)
        AND (? IS NULL OR dst_cell = ?)
        AND (? IS NULL OR state = ?)
        AND (? IS NULL OR volname = ?)
END
    my $res = $dbh->query($sql,
                          $info{jobid}, $info{jobid},
                          $info{src_cell}, $info{src_cell},
                          $info{dst_cell}, $info{dst_cell},
                          $info{state}, $info{state},
                          $info{volname}, $info{volname});
    my @jobs = $res->hashes();

    for my $job (@jobs) {
        # Parse our datetime columns into real perl datetime objects
        for my $col (qw(ctime mtime now_server errorlimit_mtime)) {
            if (defined($job->{$col})) {
                $job->{$col} = _parse_dt($dbh, $job->{$col});
            }
        }

        # Add some additional fields that can be helpful to callers
        $job->{deadline} = undef;
        $job->{expired} = 0;
        $job->{stale_seconds} = $job->{now_server}->subtract_datetime_absolute($job->{mtime})->seconds;
        $job->{age_seconds} = $job->{now_server}->subtract_datetime_absolute($job->{ctime})->seconds;

        if (defined($job->{timeout})) {
            $job->{deadline} = $job->{mtime}->clone->add(seconds => $job->{timeout});

            if (DateTime->compare($job->{now_server}, $job->{deadline}) > 0) {
                $job->{expired} = 1;
            }
        }
    }

    return @jobs;
}

# Move a (presumably finished) job from the main jobs table to the archival
# "jobs history" table.
sub
archive_job(%) {
    my %info = _check_args({@_},
        req => [qw(jobid
                   dv
        )],
        opt => [qw(dbh
        )],
    );
    my $dbh = $info{dbh};

    if (!$dbh) {
        db_rw(sub($) {
            my ($sub_dbh) = @_;
            archive_job(%info, dbh => $sub_dbh);
        });
        return;
    }

    my $sql = <<'END';
    INSERT INTO jobshist
    SELECT * FROM jobs
    WHERE id = ? AND dv = ?
END
    my $res = $dbh->query($sql, $info{jobid}, $info{dv});
    if ($res->rows() != 1) {
        die("DB Error: Updated ".$res->rows()." rows trying to archive job $info{jobid} dv $info{dv}\n");
    }
}

# Kill the specified job; that is, delete it from the main jobs table.
sub
kill_job(%) {
    my %info = _check_args({@_},
        req => [qw(jobid
        )],
        opt => [qw(dbh
                   dv
        )],
    );
    my $dbh = $info{dbh};

    if (!$dbh) {
        db_rw(sub($) {
            my ($sub_dbh) = @_;
            kill_job(%info, dbh => $sub_dbh);
        });
        return;
    }

    my $sql = <<'END';
    DELETE FROM jobs
    WHERE
        id = ?
        AND (? IS NULL OR dv = ?)
END
    my $res = $dbh->query($sql, $info{jobid}, $info{dv}, $info{dv});
    if ($res->rows() != 1) {
        my $dvstr = '';
        if (defined($info{dv})) {
            $dvstr .= " dv $info{dv}";
        }
        die("DB Error: Updated ".$res->rows()." rows trying to delete job $info{jobid}$dvstr\n");
    }
}

# This causes a job to be marked as failed (state ERROR) in the db, and so it
# is suitable for retrying by the check-server. Note that job_error() is
# considered best-effort; if we cannot contact the database, or if the provided
# dvref is stale, we will just log an error and return. We do not throw an
# exception on dvref staleness like most other DB manipulation functions.
sub
job_error(%) {
    my %info = _check_args({@_},
        req => [qw(jobid
                   dvref
        )],
        opt => [qw(dbh
        )],
    );

    my $dbh = $info{dbh};
    my $dv = ${$info{dvref}};

    if (!$dbh) {
        db_rw(sub($) {
            my ($sub_dbh) = @_;
            job_error(%info, dbh => $sub_dbh);
        });
        return;
    }

    DEBUG "job_error(jobid ".$info{jobid}.", dv $dv)";

    eval {
        my @rows = $dbh->query(
            'SELECT state, errors FROM jobs WHERE id = ? AND dv = ?',
            $info{jobid}, ${$info{dvref}})->arrays();
        if (@rows != 1) {
            ERROR "When erroring jobid $info{jobid}: Got ".@rows." rows when looking up state/errors";
            return;
        }

        my $old_state = $rows[0]->[0];
        my $old_errors = $rows[0]->[1];

        _job_set(%info,
                 last_good_state => $old_state,
                 to_state => 'ERROR',
                 errors   => $old_errors + 1,
                 timeout  => undef);
    };
    if ($@) {
        ERROR "When erroring jobid $info{jobid}: $@";
        return;
    }
}

# This is intended to be used to allow a job to be retried after it has failed.
# The specified job is immediately transitioned to the specified state, and
# error-related fields are cleared.
sub
job_reset(%) {
    my %info = _check_args({@_},
        req => [qw(jobid
                   dvref
                   to_state
        )],
        opt => [qw(dbh
                   errors
        )],
    );

    DEBUG "job_reset(".$info{jobid}.", ".${$info{dvref}}.")";

    _job_set(%info,
             last_good_state => undef,
             timeout => undef,
             errorlimit_mtime => undef);
}

# This takes a hashref describing a job, and returns a new hashref with all
# fields converted to strings. A few job description fields are objects, so
# having them converted to strings makes it easier to use when printing stuff,
# or interpolating into strings in general.
sub
jobinfo_stringify($) {
    my ($a_job) = @_;

    # Make a copy, since we're altering its fields
    my %job = %$a_job;

    # These fields are DateTime objects. Convert them to strings for use with
    # printing/stringifying the job object
    for my $field (qw(deadline ctime mtime now_server errorlimit_mtime)) {
        if (defined($job{$field})) {
            $job{$field} = "".$job{$field};
        }
    }

    return \%job;
}

1;
