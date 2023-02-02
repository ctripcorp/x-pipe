source config.sh

function usage() {
	echo "./switch.sh [clustername] [targetdc]"
}

function quit() {
	echo "ERR: $1"
	exit 1
}

function send() {
	ip=`echo "$1" | awk -F ":" '{print $1}'`
	port=`echo "$1" | awk -F ":" '{print $2}'`
	command="$2"
	echo "  - redis-cli -h $ip -p $port $2"
	resp=`redis-cli -h $ip -p $port $2`
	echo "   * $resp"
}

clustername=$1
targetdc=$2

if [ -z $1 ];then
	usage
	quit "clustername cannot be null"
fi

if [ -z $2 ];then
	usage
	quit "targetdc cannot be null"
fi

echo "running: ./switch.sh $clustername $targetdc"

echo "1. FetchMeta"
xpipemeta=`curl -s $xpipeurl/api/cluster/$clustername`
credismeta=`curl -s $credisurl/keeperApi/querycluster?name=$clustername`
dcs=`echo "$xpipemeta"|jq '.dcs'|jq '.[]'`
groups=`echo "$credismeta"|jq '.Groups'| jq -c '.[]'`

for dc in $dcs
do
        echo "dc: $dc"
done

echo "2. DeleteCluster $clustername -> xpipe"
ignore=`curl -s -X "DELETE" $xpipeurl/api/cluster/$clustername?checkEmpty=false`

echo "3. Doing Switch"


ignore=`curl -s $credisurl/stopsso`

for group in $groups
do
	groupname=`echo "$group"|jq '.Name'`
	instances=`echo "$group"|jq '.Instances'`
	masters=`echo "$instances" | jq -r '.[]|select(.IsMaster)|select (.Env == "'"$targetdc"'")|[.IPAddress,.Port|tostring]|join(":")'`
	slaves=`echo "$instances" | jq -r '.[]|select(.IsMaster != true)|select (.Env == "'"$targetdc"'")|[.IPAddress,.Port|tostring]|join(":")'`
	others="$others `echo "$instances" | jq -r '.[]|select (.Env != "'"$targetdc"'")|[.IPAddress,.Port|tostring]|join(":")'`"

	echo " - GroupName: $groupname"
	if [ -z  $masters ];then
		master=`echo "$slaves" | head -n 1`
	else
		master=`echo "$masters" | head -n 1`
	fi
	allmasters="$allmasters $master"
	master_ip_port=`echo "$master" | awk -F ":" '{print $1" "$2}'`

	send "$master" "slaveof no one"

	for slave in $slaves
	do
		if [ $slave == $master ];then
			continue
		fi
		send "$slave" "slaveof $master_ip_port"
	done

done

all_masters=`echo -n $allmasters | jq -cRs 'split(" ")'`

credisdr_resp=`curl -s -X "POST"  -H "Content-type:application/json" -d"$all_masters" "$credisurl/keeperApi/primarydc/$clustername/$targetdc"`
echo "4. CRedisDRSwitch $clustername -> $targetdc"
echo " * $credisdr_resp"

for other in $others
do
	other_ip_port=`echo "$other" | awk -F ":" '{print "ip="$1"&port="$2}'`
	markdown_resp=`curl -s -X "POST" "$credisurl/keeperApi/switchReadStatus?$other_ip_port&canRead=false&clusterName=$clustername"`
	echo "   - Markdown $other"
	echo "    * $markdown_resp"
done

ignore=`curl -s $credisurl/startsso`
