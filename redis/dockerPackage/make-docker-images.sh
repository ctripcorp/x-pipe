#!/bin/bash

#functions
function makeImages(){
    comp=$1
    if [ -f xpipe-$comp/redis-$comp.jar ];then
      rm xpipe-$comp/redis-$comp.jar
    fi

    cp ../package/redis-$comp*-package/target/redis-$comp*.jar  xpipe-$comp/redis-$comp.jar

    docker build -t $DOCKER_ID/xpipe-$comp:$DOCKER_TAG xpipe-$comp
}

#vars
DOCKER_ID=${1:-'ctripcorpxpipe'}
DOCKER_TAG=${2:-'1.0'}

mvn clean install -Plocal,package -DskipTests

cd redis/dockerPackage


makeImages console
makeImages keeper
makeImages meta
makeImages proxy
