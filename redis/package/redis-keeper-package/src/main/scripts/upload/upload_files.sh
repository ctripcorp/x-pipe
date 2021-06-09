DIR=`dirname $0`
ip=$1
local_path="$2"
remote_path=$3
remove=$4

username=xpipe
password=12qwaszx

if [ -z $local_path ]; then
    echo "Input Error. Local_path cannot be null"
    exit
fi

#remote_path="~"
#mkdir_cmd="mkdir -p $remote_path/network"
mkdir_cmd="mkdir -p $remote_path"


if [ "$remote_path" -ne "~" ] && [ "$remote_path" -ne "/home/xpipe" ]; then
    if [ "$remove" == "rm" ]; then
        echo "echo rm dir and upload"
            sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip "rm -rf $remote_path; $mkdir_cmd" 
        wait
    else
        if [ -z $remote_path ]; then
            echo "echo remote path:~"
        else
            echo "echo create remote path: $remote_path"
            sshpass -p"$password" ssh -o StrictHostKeyChecking=no $username@$ip $mkdir_cmd 
            wait
        fi
    fi
fi
sshpass -p"$password" scp -r $local_path $username@$ip:$remote_path
#sshpass -p"$password" scp -r ~/Downloads/mount.sh $username@$ip:$remote_path
#sshpass -p"$password" scp -r ../network/* $username@$ip:$remote_path/network
