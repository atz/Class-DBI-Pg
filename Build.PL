# $Id: Build.PL 6 2006-05-20 01:18:11Z daisuke $
#
# Copyright (c) 2006 Daisuke Maki <dmaki@cpan.org>
# All rights reserved.

use strict;
use Module::Build;

my $build = Module::Build->new(
    dist_name => 'Class-DBI-Pg',
    dist_version_from => 'lib/Class/DBI/Pg.pm',
    requires => {
        'Class::DBI' => '>= 0.89',
        'DBD::Pg'    => '>= 1.13',
    },
    build_requires => {
        'Test::More' => 0
    },
    license => 'perl',
    create_readme => 1,
    create_makefile_pl => 'traditional',
);
$build->create_build_script;