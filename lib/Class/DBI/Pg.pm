package Class::DBI::Pg;
# $Id: Pg.pm,v 1.11 2002/08/08 10:26:39 ikechin Exp $
use strict;
require Class::DBI;
use base 'Class::DBI';
use vars qw($VERSION);
$VERSION = '0.02';

sub _croak { require Carp; Carp::croak(@_); }

sub set_up_table {
    my($class, $table) = @_;
    my $dbh = $class->db_Main;

    # find primary key
    my $sth = $dbh->prepare(<<'SQL');
SELECT indkey FROM pg_index
WHERE indisprimary=true AND indrelid=(
SELECT oid FROM pg_class
WHERE relname = ?)
SQL
    $sth->execute($table);
    my $prinum = $sth->fetchrow_array;
    $sth->finish;

    # find all columns
    $sth = $dbh->prepare(<<'SQL');
SELECT a.attname, a.attnum
FROM pg_class c, pg_attribute a
WHERE c.relname = ?
  AND a.attnum > 0 AND a.attrelid = c.oid
ORDER BY a.attnum
SQL
    $sth->execute($table);
    my $columns = $sth->fetchall_arrayref;
    $sth->finish;

    # find SERIAL type.
    # nextval('"table_id_seq"'::text)
    $sth = $dbh->prepare(<<'SQL');
SELECT adsrc FROM pg_attrdef 
WHERE adrelid=(SELECT oid FROM pg_class WHERE relname=?)
SQL
    $sth->execute($table);
    my($nextval_str) = $sth->fetchrow_array;
    $sth->finish;
    my($sequence) = $nextval_str =~ m/^nextval\('"?([^"']+)"?'::text\)/;

    my(@cols, $primary);
    foreach my $col(@$columns) {
	push @cols, $col->[0];
	next unless $prinum && $col->[1] eq $prinum;
	$primary = $col->[0]; 
    }
    _croak("$table has no primary key") unless $primary;
    $class->table($table);
    $class->columns(Primary => $primary);
    $class->columns(All => @cols);
    $class->sequence($sequence) if $sequence;
}

1;
__END__

=head1 NAME

Class::DBI::Pg - Class::DBI extension for Postgres

=head1 SYNOPSIS

  use strict;
  use base qw(Class::DBI::Pg);

  __PACKAGE__->set_db(Main => 'dbi:Pg:dbname=dbname', 'user', 'password');
  __PACKAGE__->set_up_table('film');

=head1 DESCRIPTION

Class::DBI::Pg automate the setup of Class::DBI columns and primary key
for Postgres.

select Postgres system catalog and find out all columns, primary key and
SERIAL type column.

create table.

 CREATE TABLE cd (
     id SERIAL NOT NULL PRIMARY KEY,
     title TEXT,
     artist TEXT,
     release_date DATE
 );

setup your class.

 package CD;
 use strict;
 use base qw(Class::DBI::Pg);

 __PACKAGE__->set_db(Main => 'dbi:Pg:dbname=db', 'user', 'password');
 __PACKAGE__->set_up_table('cd');
 
This is almost the same as the following way.

 package CD;

 use strict;
 use base qw(Class::DBI);

 __PACKAGE__->set_db(Main => 'dbi:Pg:dbname=db', 'user', 'password');
 __PACKAGE__->table('cd');
 __PACKAGE__->columns(Primary => 'id');
 __PACKAGE__->columns(All => qw(id title artist release_date));
 __PACKAGE__->sequence('cd_id_seq');

=head1 AUTHOR

IKEBE Tomohiro E<lt>ikebe@edge.co.jpE<gt>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

L<Class::DBI> L<Class::DBI::mysql> L<DBD::Pg>

=cut
