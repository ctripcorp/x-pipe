IR=`dirname $0`
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
    isolated_iplist_file="$DATA_DIR/iplist_ptjq"
    isolated_a10_file="$DATA_DIR/a10ip_ptjq"
    active_iplist_file="$DATA_DIR/iplist_ptoy"
    active_a10_files=( "$DATA_DIR/a10ip_ptoy" "$DATA_DIR/a10ip_ptfq" )
    active_fq_file="$DATA_DIR/iplist_ptfq"
    third_party_iplist_file="$DATA_DIR/iplist_thirdparty"
else
    isolated_iplist_file="$DATA_DIR/iplist_ptoy"
    isolated_a10_file="$DATA_DIR/a10ip_ptoy"
    active_iplist_file="$DATA_DIR/iplist_ptjq"
    active_a10_files=( "$DATA_DIR/a10ip_ptjq" "$DATA_DIR/a10ip_ptfq" )
    active_fq_file="$DATA_DIR/iplist_ptfq"
    third_party_iplist_file="$DATA_DIR/iplist_thirdparty"
fi

while read isolated_ip
do
    echo $isolated_ip >> $DIR/iplist
        while read other_isolated_ip
        do
           # if [ $isolated_ip != $other_isolated_ip ]; then
               echo $other_isolated_ip >> $IPLIST_PATH/$isolated_ip
           # fi
        done < $isolated_iplist_file
        while read isolated_a10_ip
        do
               echo $isolated_a10_ip >> $IPLIST_PATH/$isolated_ip
        done < $isolated_a10_file
                while read third_party_ip
        do
               echo $third_party_ip >> $IPLIST_PATH/$isolated_ip
        done < $third_party_iplist_file
done < $isolated_iplist_file


while read active_ip
do
    echo $active_ip >> $DIR/iplist
        while read other_active_ip
        do
            #if [ $active_ip != $other_active_ip ]; then
               echo $other_active_ip >> $IPLIST_PATH/$active_ip
            #fi
        done < $active_iplist_file

    for active_a10_file in ${active_a10_files[@]};
    do
        while read active_a10_ip
        do
               echo $active_a10_ip >> $IPLIST_PATH/$active_ip
        done < $active_a10_file
    done
        while read active_fq_ip
        do
               echo $active_fq_ip >> $IPLIST_PATH/$active_ip
        done < $active_fq_file
        while read third_party_ip
        do
               echo $third_party_ip >> $IPLIST_PATH/$active_ip
        done < $third_party_iplist_file
done < $active_iplist_file
