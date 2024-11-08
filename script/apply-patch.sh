PATCH_PATH=$(readlink -f patch/)

# fio
pushd .
cd fio
./configure
make
popd

# filebench
pushd .
cd filebench/
patch -p1 < $PATCH_PATH/filebench.patch
libtoolize
aclocal
autoheader
automake --add-missing
autoconf
./configure
make
popd

# rocksdb & zenfs
cd rocksdb/plugin/zenfs
patch -p1 < $PATCH_PATH/zenfs.patch
cd ../../
DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs make -j48 db_bench
cd plugin/zenfs/util
make