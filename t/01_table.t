use strict;
use Test::More tests => 5;

use Class::DBI::Pg;
use DBI;

print STDERR "\n";
my $database = read_input("please specify the writable dbname");
my $user = read_input("please specify the Pg username");
my $password = read_input("please specify the Pg password");
my $dsn = "dbi:Pg:dbname=$database";
    
my $dbh = DBI->connect($dsn, $user, $password, { 
    AutoCommit => 1, PrintError => 0, RaiseError => 1,
});

$dbh->do(<<'SQL');
CREATE TABLE class_dbi_pg1 (
    id SERIAL NOT NULL PRIMARY KEY,
    dat TEXT
)
SQL

my $sth = $dbh->prepare(<<"SQL");
INSERT INTO class_dbi_pg1 (dat) VALUES(?)
SQL
for my $dat (qw(foo bar baz)) {
    $sth->execute($dat);
}
$sth->finish;
 
package Class::DBI::Pg::Test;
use base qw(Class::DBI::Pg);
__PACKAGE__->set_db(Main => $dsn, $user, $password);
__PACKAGE__->set_up_table('class_dbi_pg1');

package main;

is(Class::DBI::Pg::Test->retrieve_all, 3);
my $obj = Class::DBI::Pg::Test->retrieve(2);
is($obj->dat, 'bar');
my($obj2) = Class::DBI::Pg::Test->search(dat => 'foo');
is($obj2->id, 1);

like(Class::DBI::Pg::Test->sequence, qr/class_dbi_pg1_id_seq/);
my $new_obj = Class::DBI::Pg::Test->create({ dat => 'newone'});
is($new_obj->id, 4);

Class::DBI::Pg::Test->db_Main->disconnect;

sub read_input {
    my $prompt = shift;
    print STDERR "$prompt: ";
    my $value = <STDIN>;
    chomp $value;
    return $value;
}

END {
    if ($dbh) {
	eval {
	    unless (Class::DBI::Pg::Test->pg_version >= 7.3) {
		$dbh->do('DROP SEQUENCE class_dbi_pg1_id_seq');
	    }
	    $dbh->do('DROP TABLE class_dbi_pg1');
	};
	$dbh->disconnect;
    }
}
