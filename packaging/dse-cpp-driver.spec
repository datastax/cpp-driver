%define distnum %(/usr/lib/rpm/redhat/dist.sh --distnum)

Name:    dse-cpp-driver
Epoch:   1
Version: %{driver_version}
Release: 1%{?dist}
Summary: DataStax C/C++ DataStax Enterprise Driver

Group: Development/Tools
License: Apache 2.0
URL: https://github.com/datastax/cpp-driver
Source0: %{name}-%{version}.tar.gz
Source1: dse.pc.in
Source2: dse_static.pc.in

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
%if %{distnum} == 5
BuildRequires: buildsys-macros >= 5
%endif
BuildRequires: cmake >= 2.6.4
BuildRequires: libuv-devel >= %{libuv_version}
BuildRequires: openssl-devel >= 0.9.8e
BuildRequires: krb5-devel

%description
A driver built on top of the C/C++ driver for Apache Cassandra, with specific
extensions for DataStax Enterprise (DSE).

%package devel
Summary: Development libraries for ${name}
Group: Development/Tools
Requires: %{name} = %{epoch}:%{version}-%{release}
Requires: libuv >= %{libuv_version}
Requires: openssl >= 0.9.8e
Requires: krb5-libs
Requires: pkgconfig

%description devel
Development libraries for %{name}

%prep
%setup -qn %{name}-%{version}

%build
export CFLAGS='%{optflags}'
export CXXFLAGS='%{optflags}'
cmake -DCMAKE_BUILD_TYPE=RELEASE -DDSE_BUILD_STATIC=ON -DDSE_INSTALL_PKG_CONFIG=OFF -DCMAKE_INSTALL_PREFIX:PATH=%{_prefix} -DCMAKE_INSTALL_LIBDIR=%{_libdir} .
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} install

mkdir -p %{buildroot}/%{_libdir}/pkgconfig
sed -e "s#@prefix@#%{_prefix}#g" \
    -e "s#@exec_prefix@#%{_exec_prefix}#g" \
    -e "s#@libdir@#%{_libdir}#g" \
    -e "s#@includedir@#%{_includedir}#g" \
    -e "s#@version@#%{version}#g" \
    %SOURCE1 > %{buildroot}/%{_libdir}/pkgconfig/dse.pc
sed -e "s#@prefix@#%{_prefix}#g" \
    -e "s#@exec_prefix@#%{_exec_prefix}#g" \
    -e "s#@libdir@#%{_libdir}#g" \
    -e "s#@includedir@#%{_includedir}#g" \
    -e "s#@version@#%{version}#g" \
    %SOURCE2 > %{buildroot}/%{_libdir}/pkgconfig/dse_static.pc

%clean
rm -rf %{buildroot}

%check
# make check

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(-,root,root)
%doc README.md
%{_libdir}/*.so
%{_libdir}/*.so.*

%files devel
%defattr(-,root,root)
%doc README.md
%{_includedir}/*.h
%{_includedir}/dse/*.h
%{_libdir}/*.a
%{_libdir}/pkgconfig/*.pc

%changelog
* Wed Jun 01 2015 Michael Penick <michael.penick@datastax.com> - 1.0.0eap1-1
- eap release
