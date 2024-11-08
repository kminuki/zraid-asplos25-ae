#!/bin/bash

# fio
git clone https://github.com/axboe/fio.git
cd fio
git checkout fio-3.36
cd -

# filebench
git clone https://github.com/filebench/filebench.git
cd filebench
git checkout 22620e602cbbebad90c0bd041896ebccf70dbf5f
cd -

# RocksDB & ZenFS
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout v8.3.2
git clone https://github.com/westerndigitalcorporation/zenfs plugin/zenfs
cd -