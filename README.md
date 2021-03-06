MoSQL Storage Engine
====================

The MoSQL storage engine for MySQL/MariaDB (mosql-se) is a single component of the larger MoSQL system developed at the Unviersity of Lugano, now released under the GPL v3. For an overview of the system, please see the [main project home page](http://dslab.inf.usi.ch/mosql/). 

mosql-se requires [mosql-storage](https://bitbucket.org/atomic77/mosql-storage) to be running in order to function. The MoSQL storage layer is a fully-functioning transactional key-value system on its own, useful for applications which do not require full-fledged SQL transactions. 

Building
--------

The MoSQL storage engine code uses CMake. It is a pain to build properly because it requires the full source of MySQL/MariaDB, so we will be making binaries available for different versions of MySQL. For the brave, you will require:

* MySQL 5.1 (see the wiki/issues for support coming for newer versions)
* *Full* source for the version you intend to run, not just the development headers!
* [mosql-storage] built and installed
* libevent 2.0+

An example of how to build using CMake:

    cd mosql-se
    mkdir build
    cd build
    cmake -D CMAKE_INSTALL_PREFIX=~/local/mosql-se \ 
      -D MYSQLSRC_ROOT=~/src/mysql-5.1.51 \
      -D LIBTAPIOCA_ROOT=~/local/mosql-storage  \ 
      -D  CMAKE_BUILD_TYPE=Debug  \
      -D MYSQL_ROOT=~/local/mysql-debug-5.1.51 ../src
    

Installation
------------

Assuming CMake ran successfully, run:

    make -j 4
    make install
    
To build and install mosql-se into the target installation folder. 


Launching
---------

In the scripts folder, use launch_db.sh to copy the shared library to the appropriate folder in your mysql installation (usually <basedir>/lib/plugin or <basedir>/lib/mysql/plugin) and launch the mysqld process. 

To install the storage engine library into the MySQL server, run

    INSTALL PLUGIN mosql SONAME 'libmosqlse.so';

In order for the plugin to successfully install, it will expect to find a single configuration file called 'tapioca.cfg' in the data directory of your mysql installation. The format is simple, it should contain a single line for every storage node you wish the MySQL server to connect to in the following format:

    <Host> <Port> <Node#>

An example file where MySQL connects to two local mosql storage processes would be:

    127.0.0.1 5555 0  
    127.0.0.1 5556 1  

