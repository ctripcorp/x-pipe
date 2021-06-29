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
    src_console_file="$DATA_DIR/consoleiplist_ptjq"
    dst_console_file="$DATA_DIR/consoleiplist_ptoy"
    src_a10_ip=`cat $DATA_DIR/a10ip_ptjq`
    dst_a10_ip=`cat $DATA_DIR/a10ip_ptoy`
else
    src_iplist_file="$DATA_DIR/iplist_ptoy"
    dst_iplists_files=( "$DATA_DIR/iplist_ptjq" "$DATA_DIR/iplist_ptfq" )
    src_console_file="$DATA_DIR/consoleiplist_ptoy"
    dst_console_file="$DATA_DIR/consoleiplist_ptjq"
    src_a10_ip=`cat $DATA_DIR/a10ip_ptoy`
    dst_a10_ip=`cat $DATA_DIR/a10ip_ptjq`
fi

#src_iplist
while read src_ip
do
    #iplist=()
    echo $src_ip >> $DIR/iplist
    for iplist_file in ${dst_iplists_files[@]};
    do
        while read dst_ip
        do
            #iplist+=(dst_ip)
            echo $dst_ip >> $IPLIST_PATH/$src_ip
        done < $iplist_file
    done
    is_src_console=`grep "^$src_ip$" $src_console_file`
    if [ ${#is_src_console} -gt 0 ]; then
        #iplist+=$dst_a10_ip
        echo $dst_a10_ip >> $IPLIST_PATH/$src_ip
    fi
done < $src_iplist_file

#console
while read dst_console_ip
do
    echo $dst_console_ip >> $DIR/iplist
    echo $src_a10_ip >> $IPLIST_PATH/$dst_console_ip
done < $dst_console_file
