# VEP

## Running VEP

Hail can run VEP in parallel from within Spark. In order to gaurantee that instances of VEP will run the same
across different installations of Hail, we use a prepackaged VEP that lives in a docker container and points
to standard locations for database caches, plugin directories, etc. The version of VEP running within Hail is 
therefore _not_ the same as any VEP installation you may have already installed in your home directory.

The current set of VEP options may be found by running:
$ hail vep -h

## Configuring VEP (Basic)

It is possible to modify the VEP command line by specifying the "--vep-options" to the Vep command within Hail.
See "hail vep -h" for details. 

## Configuring VEP (Advanced)

VEP uses a pre-built docker image which contains all the libraries (PERL, samtools, mysql, etc.) required to run 
VEP with our default settings. We also point to a configuration directory that contains caches and other large files that
would be inefficient to put in docker. (This includes the contents of the directory usually found
at ~/.vep). Advanced users can override both of these defaults.

### Building a new docker image

To build a new docker image you need to modify the Dockerfile:
 - cd ${Hail_Project_Dir}/resources/docker/vep-build
 - Modify Dockerfile as you need to. (See: https://docs.docker.com/engine/reference/builder/)
 - Run `docker build -t vep-v1 .` This builds the image and stores it as "vep-v1". You may want to change this name.
 - Make this docker image available in whatever docker repository is used by the nodes in your Spark cluster. 
 - ${Hail_Project_Dir}/build/install/hail/bin/hail import -i ~/hail/src/test/resources/sample.vcf vep

### Modifying the configuration directory

It is possible to specify a different configuration directory simply by passing in an appropriate --vep-config parameter.
This allows you to install new VEP plugins, new reference caches, etc. If making non-trivial changes to the configuration
directory be aware that a related change to the docker image may be required. For example, if a particular VEP plugin
requires a certain PERL module to run, or a program like bzip2, then you will need to follow the above instructions for 
adding that PERL module or external program to the Dockerfile. It is _not_ enough to simply assume that modules or programs
will be available merely because they exist on your linux workstation or local cluster. This does, unfortunately, mean that
it can be a little more work to install different modules into VEP, but the payoff is that the Hail system stays portable among
different users and computing environments.

