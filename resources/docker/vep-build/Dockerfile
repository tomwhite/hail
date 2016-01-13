FROM ubuntu
RUN apt-get update

#Install packages need by VEP
RUN apt-get -y install zip
RUN apt-get -y install build-essential
RUN apt-get -y install zlib1g-dev
RUN apt-get -y install curl

# Install MySQL, turning off interactive prompts
RUN export DEBIAN_FRONTEND=noninteractive
RUN apt-get -y install mysql-server
RUN apt-get -y install libmysqlclient-dev

#Install perl packages needed by VEP, turning off interactive prompts
RUN export PERL_MM_USE_DEFAULT=1
RUN cpan install DBI
RUN cpan install Archive::Zip
RUN cpan install DBD::mysql

# Install VEP
ENV vepzip ensembl-tools-release-83.zip
ADD ./$vepzip /opt/$vepzip
WORKDIR /opt
RUN ls
RUN pwd
RUN unzip $vepzip
RUN rm $vepzip
WORKDIR /opt/ensembl-tools-release-83/scripts/variant_effect_predictor/
RUN perl INSTALL.pl --NO_HTSLIB

# Install samtools
RUN apt-get -y install libncurses-dev
ADD ./samtools-1.2.tar.bz2 /opt/
WORKDIR /opt/samtools-1.2
RUN make

# Needed for pulling data out of Vep
RUN cpan install JSON:XS
