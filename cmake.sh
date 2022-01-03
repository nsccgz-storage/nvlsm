#!/bin/bash

clear_build=$1
build_type=$2
build_type_debug="debug"
build_type_release="release"
clear_str="clear"

#echo $clear_build
Build() {
	cd build
	if [ "$1" = "$build_type_debug" ]; then
		echo "bulid type: debug" 
		cmake3 -DCMAKE_BUILD_TYPE=Debug .. && cmake3 --build . -j
	else
		echo "build type: release"
		cmake3  -DMAKE_BUILD_TYPE=Release .. && cmake3 --build . -j
	fi

}
if [ "$clear_build" = "$clear_str" ]; then
	echo "clear build dir and rebuild"
	rm -rf ./build
	mkdir build 
	pwd
else
	echo "just rebuild"
fi


Build $build_type

