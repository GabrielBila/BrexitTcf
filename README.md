## Synopsis

Brexit TCF - Scala program that downloads all tweets related to Brexit from the Twitter API. Specify the dates between
which to download the tweets when launching it.

## Prerequisites

1. JDK 8 installed (make sure JAVA_HOME points to the right jdk directory)
1. Scala installed (latest version)
1. Gradle installed

## Installation

After copying the Git repository on your local machine, ```cd``` to the project directory.

To deploy the application, run ```gradle installDist```. The generated JAR files and launch scripts will be 
placed in ```build/install/BrexitTcf```

## API Reference

To launch the application, ```cd``` to ```build/install/BrexitTcf/bin``` and run
```./BrexitTcf <start-date> <end-date>```. For example ```./BrexitTcf 2016-04-10 2016-04-12```.

This will launch the application, and it will query the Twitter API for #Brexit-related tweets. The tweets will be
output in the current directory. One separate JSON file will be created for each day.