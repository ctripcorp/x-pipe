DIR=`dirname $0`
DATA_DIR="$DIR/data"
IPLIST_PATH="$DIR/iplists"
IPLIST_FILE="$DIR/iplist"

rm -rf $IPLIST_PATH
rm $IPLIST_FILE
mkdir -p $IPLIST_PATH

dc=$1

if [ "ptjq" != "$dc" ] && [ "ptoy" != "$dc" ]; then
    echo "Input error. DC param must be 'ptjq' or 'ptoy'"
    exit
fi

if [ "ptjq" == "$dc" ]; then
    src_iplist_file="$DATA_DIR/iplist_ptjq"
    dst_iplists_files=( "$DATA_DIR/iplist_ptoy" "$DATA_DIR/iplist_ptfq" )
    src_beacon_file="$DATA_DIR/beaconiplist_ptjq"
    dst_beacon_files=( "$DATA_DIR/beaconiplist_ptoy" "$DATA_DIR/beaconiplist_ptfq" )
else
    src_iplist_file="$DATA_DIR/iplist_ptoy"
    dst_iplists_files=( "$DATA_DIR/iplist_ptjq" "$DATA_DIR/iplist_ptfq" )
    src_beacon_file="$DATA_DIR/beaconiplist_ptoy"
    dst_beacon_files=( "$DATA_DIR/beaconiplist_ptjq" "$DATA_DIR/beaconiplist_ptfq" )
fi

#src_iplist
while read src_ip
do
    echo $src_ip >> $IPLIST_FILE
    is_src_beacon=`grep "^$src_ip$" $src_beacon_file`
    if [ ${#is_src_beacon} -gt 0 ]; then
        for iplist_file in ${dst_iplists_files[@]};
        do
            while read dst_ip
            do
                echo $dst_ip >> $IPLIST_PATH/$src_ip
            done < $iplist_file
        done
    else
        for dst_beacon_file in ${dst_beacon_files[@]};
        do
            while read dst_beacon_ip
            do
                echo $dst_beacon_ip >> $IPLIST_PATH/$src_ip
            done < $dst_beacon_file
        done
    fi
done < $src_iplist_file
