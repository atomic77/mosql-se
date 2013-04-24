MoSQL Storage Engine
====================

The MoSQL storage engine for MySQL/MariaDB (mosql-se) requires the mosql-storage system to be running in order to function. The MoSQL storage layer is a fully-functioning transactional key-value system on its own, useful for applications which do not require full-fledged SQL transactions. 

Building
--------

The MoSQL storage engine uses CMake. It is a pain to build properly because it requires the full source of MySQL/MariaDB, so another option is to simply download a pre-built version of the shared library from (link goes here). For the brave, you will require:

* MySQL/MariaDB 5.0+
* *Full* source for the version you intend to run, not just the development headers!
* mosql-storage (git link)
* libevent 2.0+

An example of how to build using CMake:

    cd mosql-se
    mkdir build
    cd build
    cmake -D CMAKE_INSTALL_PREFIX=/home/atomic/local/mosql-se \ 
      -D MYSQLSRC_ROOT=/home/atomic/src/mysql-5.1.51 \
      -D LIBTAPIOCA_ROOT=/home/atomic/local/mosql-storage  \ 
      -D  CMAKE_BUILD_TYPE=Debug  \
      -D MYSQL_ROOT=/home/atomic/local/mysql-debug-5.1.51 ../src
    

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

    INSTALL PLUGIN mosql SONAME 'ha_tapioca.so';

In order for the plugin to successfully install, it will expect to find a single configuration file called 'tapioca.cfg' in the data directory of your mysql installation. The format is simple, it should contain a single line for every storage node you wish the MySQL server to connect to in the following format:

    <Host> <Port> <Node#>

An example file where MySQL connects to two local mosql storage processes would be:

    127.0.0.1 5555 0  
    127.0.0.1 5556 1  

