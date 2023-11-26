#!/bin/bash
mkdir -p build
cd build
cmake -DSERVER:BOOL=ON .. # build on server
make -j16
cd ..
