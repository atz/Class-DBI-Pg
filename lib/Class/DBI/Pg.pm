package Class::DBI::Pg;
# $Id: Pg.pm,v 1.4 2002/08/08 04:46:33 ikechin Exp $
use strict;
require Class::DBI;
use base 'Class::DBI';
use Carp ();
use vars qw($VERSION);
$VERSION = '0.01';

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
    my(@cols, $primary);
    foreach my $col(@$columns) {
	push @cols, $col->[0];
	next unless $prinum && $col->[1] eq $prinum;
	$primary = $col->[0]; 
    }
    Carp::croak "$table has no primary key" unless $primary;
    $class->table($table);
    $class->columns(Primary => $primary);
    $class->columns(All => @cols);
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

select Postgres system catalog and find out all columns and primary key.

=head1 AUTHOR

IKEBE Tomohiro E<lt>ikebe@edge.co.jpE<gt>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 SEE ALSO

L<Class::DBI> L<Class::DBI::mysql> L<DBD::Pg>

=cut
