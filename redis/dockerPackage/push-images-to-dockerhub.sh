#!/bin/bash

#functions
function copyJar(){
    comp=$1
    if [ -f xpipe-$comp/redis-$comp.jar ];then
      rm xpipe-$comp/redis-$comp.jar
    fi

    cp ../package/redis-$comp*-package/target/redis-$comp*.jar  xpipe-$comp/redis-$comp.jar
}

function makeImagesAndPush(){
    comp=$1
    if [ -f xpipe-$comp/redis-$comp.jar ];then
      rm xpipe-$comp/redis-$comp.jar
    fi

    cp ../package/redis-$comp*-package/target/redis-$comp*.jar  xpipe-$comp/redis-$comp.jar

    docker build -t ctripcorpxpipe/xpipe-$comp:$DOCKER_TAG xpipe-$comp
    docker push ctripcorpxpipe/xpipe-$comp:$DOCKER_TAG
}

#vars
DOCKER_TAG=1.0
COMPOSE_FILE=docker-compose.yml

if [ $1 ];then
  DOCKER_TAG=$1
fi

mvn clean install -Plocal,package -DskipTests

cd redis/dockerPackage

docker login -u ctripcorpxpipe -p 12qwaszx#

makeImagesAndPush console
makeImagesAndPush keeper
makeImagesAndPush meta
makeImagesAndPush proxy
