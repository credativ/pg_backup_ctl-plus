%define upstream_pgdg_version 15
%define major_version 0.1
%define patchlevel 0

Name: pg_backup_ctl-plus
Version: %{major_version}
Release: %{patchlevel}%{?dist}
Summary: An advanced streaming backup tool for PostgreSQL written in C++
License: GPLv3
Source:  pg_backup_ctl-plus-%{version}.tar.gz
BuildArch: i686 x86_64
Provides: pg_backup_ctl-plus libpgbckctl-common libpgbckctl-proto

%description

pg_backup_ctl-plus is a PostgreSQL streaming backup utility, dedicated
to the streaming protocol for PostgreSQL starting with version 9.6. It offers
a core library which implements the whole functionality and a command line
interface for easy use.

%package cli
Summary: An advanced streaming backup tool for PostgreSQL written in C++, command line tool
Group:   Application/Databases
BuildRequires: popt-devel readline-devel gcc-c++ ccache cmake3 make systemd-rpm-macros
Requires: libpgbckctl-common libpgbckctl-proto zstd xz
Provides: pg_backup_ctl-plus

%description cli

pg_backup_ctl-plus is a streaming backup tool dedicated to take physical backups
from a PostgreSQL instance starting with version 9.6. It implements a transaction log
archive utility as well as basebackup management.

%pre cli

##
## Prepare the postgres user. This is adapted from Devrim's PGDG RPMs, since
## we want to be compatible as tight as we can be.
##
groupadd -g 26 -o -r postgres >/dev/null 2>&1 || :
useradd -M -g postgres -o -r -d /var/lib/pgsql -s /bin/bash \
        -c "PostgreSQL Server" -u 26 postgres >/dev/null 2>&1 || :


%post cli
##
## prepare sqlite catalog database. this is located in /var/lib/pg_backup_ctl-plus/pg_backup_ctl.sqlite and
## should have 0600 permissions with postgres ownership
##
echo "Preparing catalog database"
install -m 0700 -o postgres -d $RPM_BUILD_ROOT/%{_sharedstatedir}/pg_backup_ctl-plus

## Install the SQLite database, but check if it's already existing.
[ -e $RPM_BUILD_ROOT/%{_sharedstatedir}/pg_backup_ctl-plus/pg_backup_ctl.sqlite ] || sqlite3 $RPM_BUILD_ROOT/%{_sharedstatedir}/pg_backup_ctl-plus/pg_backup_ctl.sqlite < $RPM_BUILD_ROOT/%{_datarootdir}/pg_backup_ctl-plus/catalog.sql
chown postgres.postgres $RPM_BUILD_ROOT/%{_sharedstatedir}/pg_backup_ctl-plus/pg_backup_ctl.sqlite
chmod 0600 $RPM_BUILD_ROOT/%{_sharedstatedir}/pg_backup_ctl-plus/pg_backup_ctl.sqlite

%systemd_post pgbckctl-launcher
%systemd_post pgbckctl-walstreamer@

%files cli
%defattr(-,root,root,-)
%attr(755,root,root) %{_bindir}/pg_backup_ctl++
%attr(644,root,root) %{_datarootdir}/pg_backup_ctl-plus/catalog.sql
%attr(644,root,root) %{_unitdir}/pgbckctl-launcher.service
%attr(644,root,root) %{_unitdir}/pgbckctl-walstreamer@.service
%{_tmpfilesdir}/pg_backup_ctl-plus-tempfiles.conf

%prep
%setup -q

%build
mkdir build
cd build
CXXFLAGS="-D__DEBUG__ -D__DEBUG_XLOG__" \
        CXX="ccache g++" \
        cmake3 -DPG_BACKUP_CTL_SQLITE:FILEPATH=/var/lib/pg_backup_ctl-plus/pg_backup_ctl.sqlite \
        -DPG_CONFIG=/usr/pgsql-%{upstream_pgdg_version}/bin/pg_config \
        -DCMAKE_INSTALL_PREFIX:PATH=/usr \
        -DCMAKE_SKIP_RPATH=TRUE \
        -DSYSTEMD_SERVICE_FILE=/usr/lib/systemd/system \
        ..
make

%install
cd build
make DESTDIR=$RPM_BUILD_ROOT install

%package -n libpgbckctl-common
Version: %{major_version}
Release: %{patchlevel}%{?dist}
Summary: An advanced streaming backup tool for PostgreSQL written in C++, core library
Group:   Application/Databases
License: GPLv3
BuildRequires: cmake3 gcc-c++ make ccache boost-devel gcc-c++ ccache gettext-devel sqlite-devel postgresql%{upstream_pgdg_version}-devel systemd-rpm-macros
Requires: boost-regex boost-filesystem boost-iostreams boost-date-time boost-chrono sqlite postgresql%{upstream_pgdg_version}-libs
Provides: libpgbckctl-common.so()(64bit)

%description -n libpgbckctl-common

pg_backup_ctl-plus is a streaming backup tool dedicated to take physical backups
from a PostgreSQL instance starting with version 9.6. libpgbckctl-common is the core
library, implementing all infrastructure for PostgreSQL streaming backups.

%files -n libpgbckctl-common
%defattr(-,root,root,-)
%attr(644,root,root) %{_libdir}/libpgbckctl-common.so

%post -n libpgbckctl-common

%package -n libpgbckctl-proto
Version: %{major_version}
Release: %{patchlevel}%{?dist}
Summary: An advanced streaming backup tool for PostgreSQL written in C++, protocol support
Group:   Application/Databases
License: GPLv3
BuildRequires: cmake3 gcc-c++ make ccache boost-devel gcc-c++ ccache gettext-devel sqlite-devel postgresql%{upstream_pgdg_version}-devel systemd-rpm-macros liburing-devel
Requires: boost-regex boost-filesystem boost-iostreams boost-date-time boost-chrono sqlite postgresql%{upstream_pgdg_version}-libs
Requires: libpgbckctl-common
Provides: libpgbckctl-proto.so()(64bit)

%description -n libpgbckctl-proto

pg_backup_ctl-plus is a streaming backup tool dedicated to take physical backups
from a PostgreSQL instance starting with version 9.6. libpgbckctl-proto provides the streaming
API implementation.

%files -n libpgbckctl-proto
%defattr(-,root,root,-)
%attr(644,root,root) %{_libdir}/libpgbckctl-proto.so

%post -n libpgbckctl-proto

%clean
rm -rf $RPM_BUILD_ROOT

%changelog
* Mon Dec 30 2019 - Bernd Helmle <bernd.helmle@credativ.de> 0.1-2
- Subpackage for libpgbckctl-proto.so

* Wed Apr 25 2018 - Bernd Helmle <bernd.helmle@credativ.de> 0.1-2
- Initial testing release
