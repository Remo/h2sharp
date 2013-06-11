@echo off

if "%IKVM_HOME%" == "" (
	echo The IKVM_HOME environment variable is not configured
	echo Please make sure you installed IKVM.NET from http://www.ikvm.net/download.html, define IKVM_HOME and relaunch.
	exit 1
)

if not exist "%IKVM_HOME%\bin" (
	echo The IKVM_HOME environment variable is not configured to a directory that has a "bin" subdirectory.
	echo Please make it point to the IKVM root directory and relaunch.
	exit 1
)
