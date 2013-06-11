#!/bin/sh

echo "#
# H2Sharp bootstrap script
#"

if [[ ! -d "$IKVM_HOME" ]] ; then
	echo "[H2Sharp] IKVM.NET was not found. Please download it and set the IKVM_HOME environment variable correctly (to the parent of the 'bin' directory)"
	echo "[H2Sharp] IKVN.NET can be downloaded from http://www.ikvm.net/download.html"
	exit 1 ;
fi

H2_LIBS_OUT=Dlls

## Create output dir
#if [[ ! -d $H2_LIBS_OUT ]] ; then mkdir $H2_LIBS_OUT ; fi

cd $H2_LIBS_OUT

if [[ -e h2.jar ]] ; then
	echo "[H2Sharp] h2.jar exists and will be used to generate h2.dll."
	echo "[H2Sharp] If you wish to automatically download the latest version of h2, simply delete h2.jar and re-run" ;
else
	rm $H2_LIBS_OUT/*.jar

	echo "[H2Sharp] Downloading latest h2 JAR"
	#wget http://repo2.maven.org/maven2/com/h2database/h2/1.2.135/h2-1.2.135.jar
	wget http://www.h2database.com/automated/h2-latest.jar ;
fi

echo "[H2Sharp] Converting h2's JAR to a DLL"
mono "$IKVM_HOME/bin/ikvmc.exe" -target:library *.jar

if [[ $? == 127 ]] ; then 
	echo "[H2Sharp] Mono could not be found in the path."
	echo "[H2Sharp] Please download and install from http://www.go-mono.com/mono-downloads/download.html, then re-run"
	exit 1 ;
fi

echo "[H2Sharp] Copying IKVM.NET's runtime DLLs"
for D in $IKVM_HOME/bin/*.dll ; do cp $D . ; done
