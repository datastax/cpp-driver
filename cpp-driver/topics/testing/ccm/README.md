# Cassandra Cluster Manager (CCM)
[CCM] is a script/library used to assist in setup and teardown of Apache
Cassandra on a local machine. In an effort to reduce inconsistencies and create
a repeatable testing environment [Vagrant] can be utilized to start and stop a
[Virtual Box] VM for integration testing.

## CCM Cluster by way of Vagrant and Virtual Box
CCM Cluster is a 64-bit Ubuntu 14.04 VM. This VM comes configured with ant,
git, maven, python, CCM, JDK v1.8 Update (Latest), and Java Cryptography
Extension (JCE) Unlimited Strength Jurisdiction Policy Files v8

**NOTE:** The JCE is required for Secure Sockets Layer (SSL) testing.

The VM contains the following specifications:

- 4GB of RAM
- 32MB of Video RAM
- 4 Cores
- Hostname: ccm-cluster
- Username: vagrant
- Password: vagrant
- 6 Network Interfaces Cards (NICs)
 - Node 1: 192.168.33.11
 - Node 2: 192.168.33.12
 - Node 3: 192.168.33.13
 - Node 4: 192.168.33.14
 - Node 5: 192.168.33.15
 - Node 6: 192.168.33.16

```ruby
# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# Inline provision script
CCM_PROVISION_SCRIPT = <<EOF
#!/bin/bash

#Install package updates
printf "Installing System Packages ...\n"
#Add JDK repository and update packages
add-apt-repository ppa:webupd8team/java -y > /dev/null 2>&1
apt-get update -qq

#Auto accept the the Java license aggreement
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections

#Install the packages
apt-get install ant git maven oracle-java8-installer oracle-java8-unlimited-jce-policy python-dev python-pip -qq

#Install CCM and its dependencies
printf "Installing CCM and its dependencies ...\n"
pip install -q ccm psutil pyyaml six > /dev/null 2>&1
EOF

##
# Configure a 6 node Cassandra Cluster Manager (CCM) Virtual Machine (VM) with
# the following settings:
#
#   - 4GB of RAM
#   - 32MB of Video RAM
#   - 4 cores (CPUs)
#   - Hostname: ccm-cluster
#   - Username: vagrant
#   - Password: vagrant
#   - 6 Network Interfaces Cards (NICs)
#     - Node 1: 192.168.33.11
#     - Node 2: 192.168.33.12
#     - Node 3: 192.168.33.13
#     - Node 4: 192.168.33.14
#     - Node 5: 192.168.33.15
#     - Node 6: 192.168.33.16
##
Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  # Create Ubuntu 14.04 LTS VM
  config.vm.box = "ubuntu/trusty64"

  # Define the hostname and IP addresses (6 node cluster)
  config.vm.define "ccm-cluster" do |ccm_cluster|
    ccm_cluster.vm.hostname = "ccm-cluster"
    ccm_cluster.vm.network "private_network", ip: "192.168.33.11"
    ccm_cluster.vm.network "private_network", ip: "192.168.33.12"
    ccm_cluster.vm.network "private_network", ip: "192.168.33.13"
    ccm_cluster.vm.network "private_network", ip: "192.168.33.14"
    ccm_cluster.vm.network "private_network", ip: "192.168.33.15"
    ccm_cluster.vm.network "private_network", ip: "192.168.33.16"
  end

  # Prepare/Provision the VM
  config.vm.provision :shell do |root_provision|
    root_provision.privileged = true
    root_provision.inline = CCM_PROVISION_SCRIPT
  end

  # VM parameters for the CCM cluster
  config.vm.provider :virtualbox do |provider|
    provider.name = "ccm-cluster"
    provider.customize ["modifyvm", :id, "--memory", "4096"]
    provider.customize ["modifyvm", :id, "--vram", "32"]
    provider.customize ["modifyvm", :id, "--cpus", "4"]
    provider.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    provider.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
  end
end
```

### Starting CCM Cluster VM
After installing [Vagrant] and [Virtual Box], copy the above script into a
directory (e.g. ccm_cluster) and ensure it is named `Vagrantfile.` To start the
CCM cluster VM run the following command in the directory with the Vagrant
script file:

```bash
vagrant up
```

### Stopping/Suspending CCM Cluster VM
To stop the CCM cluster VM run the following command in the directory with the
Vagrant script file:

```bash
vagrant halt
```

To speed up launch times of the CCM cluster VM a suspend command can be issued
after the instance is first created by running the following command in the
directory with the Vagrant script file:

```bash
vagrant suspend
```

### Resuming the Suspended CCM Cluster VM
If the CCM cluster VM was suspended run the following command in the directory
with the Vagrant script file to resume:

```bash
vagrant resume
```

[CCM]: https://github.com/pcmanus/ccm
[Vagrant]: https://www.vagrantup.com/downloads.html
[Virtual Box]: https://www.virtualbox.org/wiki/Downloads
