#!/bin/bash

function getXpipeRoot() {
    currentDir=$1
    target_dir="$( cd -P "$( dirname "$currentDir/../../../../../../../" )" && pwd )"
    echo "$target_dir"
}

######################### Enter the x-pipe parent folder ####################
cd "$(dirname "$0")"
current_dir=${PWD}
root_dir=`getXpipeRoot ${current_dir}`
cd "${root_dir}"

########################  Maven build #######################################
echo "mvn build xpipe ..."
mvn clean install -Ppackage,ctrip -DskipTests 2>&1>/dev/null

echo "mvn clean install finished"


########################  Enter maven target folder & extract the .zip file ################
target_dir="$root_dir/redis/package/redis-console-package/target"

echo "target dir: $target_dir"

for file in `ls ${target_dir}`
do
    if [[ $file == *.zip ]];then
        zip_file=${file}
    fi

done

echo "zip file: $zip_file"

cd "$target_dir"

echo `mkdir -p redis-console & tar -vzxf ${zip_file} -C redis-console`

cd redis-console/scripts

#######################   Run start.sh to start console ##################################
echo "Starting xpipe console..."

echo `sh startup.sh`

sleep 60   # sleep 1 min making program fully initialized



######################   Stop the console service and calculate the elapsed time ##############

start_time="$(date -u +%s)"
echo `sh shutdown.sh`
end_time="$(date -u +%s)"


duration=`expr ${end_time} - ${start_time}`
echo "It takes $duration seconds to shutdown ..."

echo "$duration"


#####################  Remove the unzip folder  ######################################
cd ../..

rm -rf redis-console


