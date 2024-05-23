#!/bin/bash

AGE_PATH=$(dirname $(readlink -f "$0"))

# Parameters.
usage() {
    echo "Usage: $0 [-p <prefix>] [-c]";
    echo "  Options:";
    echo "    -p prefix     Set CMAKE_PREFIX_PATH for cmake";
    echo "    -c            Clean the previous building files";
    exit 1;
}

while getopts ":p:c" opt; do
    case "${opt}" in
        p)
            PREFIX=${OPTARG}
            ;;
        c)
            CLEAN="y"
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ "$CLEAN" = "y" ]; then
    echo "Cleaning building files..."
    cd $AGE_PATH;
    rm -rf deps/incubator-brpc/build;
    rm -rf build;
    rm -rf src/proto/*.pb.cc
    rm -rf src/proto/*.pb.h
    exit 1;
fi

echo "PREFIX = ${PREFIX}"

# get submodule
cd $AGE_PATH
git submodule init
git submodule update

# Compile libcypher-parser
cd $AGE_PATH
if [ ! -f "deps/libcypher-parser/lib/src/.libs/libcypher-parser.a" ]; then
    echo "[Dependency] Building libcypher-parser..."
    cd deps/libcypher-parser
    ./autogen.sh
    ./configure --disable-shared;
    make CFLAGS="-fPIC -DYY_BUFFER_SIZE=1048576" clean check
    cd ../..
fi

# Compile AGE
cd $AGE_PATH
# rm -rf bin
# rm -rf graphdata
# rm -rf build
mkdir build
cd build
if cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=$PREFIX -DCMAKE_EXPORT_COMPILE_COMMANDS=ON; then
	echo "Building AGE.."
	make -j
fi
