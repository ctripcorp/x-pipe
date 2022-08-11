import json
import time
import requests
import sys

from requests.api import get

XPIPE_HOST = 'http://127.0.0.1:8080'

# api
PATH_GET_UNHEALTHY_INFO = "/api/redis/inner/unhealthy/all"
PATH_KEEPERS_CHANGE = "/api/keepers/%s/%s/%s"
PATH_GET_CLUSTER_INFO = "/api/cluster/%s"
PATH_CHECK_KEEPER = "/api/keepers/check"

# console
PATH_GET_SHARD_DETAIL = "/console/clusters/%s/dcs/%s/shards/%s"
PATH_GET_AVAILABLE_KEEPERS = "/console/dcs/%s/availablekeepers"
PATH_GET_ACTIVE_KEEPERS = "/console/dcs/%s/cluster/%s/activekeepercontainers"
PATH_UPDATE_SHARD = "/console/clusters/%s/dcs/%s/shards/%s"

headers = {
    'Cookie':''
}

INTERVAL_SECOND_BETWEEN_SHARD = 0
INTERVAL_SECOND_BETWEEN_CLUSTER = 0
KEEPER_PORT_INIT=5000

strictReplace = True
unhealthyInfo = {}
useKeeperPorts = {}
limit = -1

def refreshUnhealthyInfo():
    global unhealthyInfo
    unhealthyInfo = requests.get(XPIPE_HOST + PATH_GET_UNHEALTHY_INFO).json()
    print("unhealthy clusters: %d"%unhealthyInfo.get("unhealthyCluster"))
    print("unhealthy shards: %d"%unhealthyInfo.get("unhealthyShard"))
    print("attach fail dcs: " + json.dumps(unhealthyInfo.get("attachFailDc")))

def showAllUnhealthyClusters():
    if (len(unhealthyInfo) <= 0):
        print("refresh unhealthy info first")
        return

    for cluster in unhealthyInfo.get("unhealthyInstance").keys():
        print(cluster)

def showAllUnhealthyShards(cluster):
    if (len(unhealthyInfo) <= 0):
        print("refresh unhealthy info first")
        return

    if cluster not in unhealthyInfo.get("unhealthyInstance"):
        print("no unhealthy shards for " + cluster)
        return

    for shard in unhealthyInfo.get("unhealthyInstance").get(cluster).keys():
        print(shard)

def getClusterInfo(cluster):
    return requests.get(XPIPE_HOST + PATH_GET_CLUSTER_INFO%cluster).json()

def getUnhealthyClusters():
    if (len(unhealthyInfo) <= 0):
        print("refresh unhealthy info first")
        return []

    return unhealthyInfo.get("unhealthyInstance").keys()

def getUnhealthyShards(cluster):
    if (len(unhealthyInfo) <= 0) or cluster not in unhealthyInfo.get("unhealthyInstance"):
        return {}

    shards = {}
    for dc_shard in unhealthyInfo.get("unhealthyInstance").get(cluster).keys():
        info = dc_shard.split(" ")
        dc = info[0]
        shard = info[1]
        if shard not in shards.keys():
            shards[shard] = []

        shards[shard].append(dc)

    return shards

def deleteKeepers(dc, cluster, shard):
    res = requests.delete(XPIPE_HOST + PATH_KEEPERS_CHANGE%(dc,cluster,shard)).json()
    return res.get('state') == 0

def addKeepers(dc, cluster, shard):
    res = requests.post(XPIPE_HOST + PATH_KEEPERS_CHANGE%(dc,cluster,shard)).json()
    return res.get('state') == 0

def checkLimit():
    global limit
    if limit <= 0:
        return False
    limit = limit - 1
    if limit == 0:
        print("replace over limit, stop")
        return True
    return False

def fixCluster(cluster):
    print("fix cluster " + cluster)
    shards = getUnhealthyShards(cluster)
    if (len(shards) == 0):
        print("no unhealthy shards for %s skip"%cluster)
        return

    clusterInfo = getClusterInfo(cluster)
    if 'clusterType' not in clusterInfo.keys():
        print("unfind cluster %s skip"%cluster)
        return
    if (clusterInfo.get('clusterType').lower() != 'one_way'):
        print("cluster not one_way cluster, skip")
        return

    activeDc = clusterInfo.get('dcs')[0]
    for (shard,dcs) in shards.items():
        if activeDc in dcs:
            print("%s %s activeDc unhealthy, skip"%(cluster, shard))
            continue
        if strictReplace:
            strictReplaceKeepers(cluster, shard, [activeDc])
            strictReplaceKeepers(cluster, shard, dcs)
        else:
            replaceKeepers(cluster, shard, [activeDc])
            replaceKeepers(cluster, shard, dcs)
        if checkLimit():
            return

def replaceKeepers(cluster, shard, dcs):
    for dc in dcs:
        print("[%s][%s][%s] begin replace keeper"%(dc, cluster, shard))
        if not deleteKeepers(dc, cluster, shard):
            print ("[%s][%s][%s]delete keepers fail , skip"%(dc, cluster, shard))
            continue
        if not addKeepers(dc, cluster, shard):
            print ("[%s][%s][%s]add keepers fail"%(dc, cluster, shard))
        time.sleep(INTERVAL_SECOND_BETWEEN_SHARD)

def strictReplaceKeepers(cluster, shard, dcs):
    for dc in dcs:
        print("[%s][%s][%s] begin strict replace keeper"%(dc, cluster, shard))
        shardInfo = getShardInfo(cluster, dc, shard)
        availableKeepers = getAvailableKeepers(shardInfo, dc)
        if len(availableKeepers) <= 0:
                print("[%s][%s][%s]no available keepers"%(dc, cluster, shard))
                continue
        # if availableKeepers.has_key('exception'):
        #     print("[%s][%s][%s]get available keeper fail %s"%(dc, cluster, shard, availableKeepers.get('message')))
        #     strictReplaceShardKeeper(cluster, dc, shard, shardInfo, [])
        else:
            strictReplaceShardKeeper(cluster, dc, shard, shardInfo, availableKeepers)
        time.sleep(INTERVAL_SECOND_BETWEEN_SHARD)

def getShardInfo(cluster, dc, shard):
    return requests.get(XPIPE_HOST + PATH_GET_SHARD_DETAIL%(cluster, dc, shard), headers = headers).json()

def getAvailableKeepers(shardInfo, dc):
    return requests.post(XPIPE_HOST + PATH_GET_AVAILABLE_KEEPERS%dc, json = shardInfo, headers = headers).json()

def getActiveKeepers(cluster, dc):
    return requests.get(XPIPE_HOST + PATH_GET_ACTIVE_KEEPERS%(dc, cluster), headers = headers).json()

def chooseKeeperPort(keeperIp):
    global useKeeperPorts
    if not useKeeperPorts.has_key(keeperIp):
        useKeeperPorts[keeperIp] = KEEPER_PORT_INIT
        return KEEPER_PORT_INIT
    else:
        useKeeperPorts[keeperIp] = useKeeperPorts[keeperIp] + 1
        return useKeeperPorts[keeperIp]

def existKeeper(keeperIp, keeperPort):
    res = requests.post(XPIPE_HOST + PATH_CHECK_KEEPER, json={
        "host":keeperIp,
        "port":keeperPort
    }).json()
    return res.has_key("payload") and res.get("payload")

def strictReplaceShardKeeper(cluster, dc, shard, shardInfo, availableKeepers):
    newKeepers = []
    for availableKeeper in availableKeepers:
        useKeeper = True
        for currentKeeper in shardInfo.get('keepers'):
            if currentKeeper.get('redisIp') == availableKeeper.get('redisIp'):
                useKeeper = False
        if useKeeper:
            newKeepers.append(availableKeeper)

    if len(newKeepers) < 2:
        activeKeepers = getActiveKeepers(cluster, dc)
        for activeKeeper in activeKeepers:
            if len(newKeepers) >= 2:
                break
            useKeeper = True
            for currentKeeper in shardInfo.get('keepers'):
                if currentKeeper.get('redisIp') == activeKeeper.get('keepercontainerIp'):
                    useKeeper = False
            for newKeeper in newKeepers:
                if newKeeper.get('redisIp') == activeKeeper.get('keepercontainerIp'):
                    useKeeper = False
            if useKeeper:
                keeperIp = activeKeeper.get('keepercontainerIp')
                keeperPort = chooseKeeperPort(keeperIp)
                while existKeeper(keeperIp, keeperPort):
                    keeperPort = chooseKeeperPort(keeperIp)
                    print("[%s][%s][%s]already exist keeper %s %d, skip"%(dc, cluster, shard,keeperIp, keeperPort))
                    time.sleep(0.1)
                newKeepers.append({"keepercontainerId": activeKeeper.get("keepercontainerId"), "redisPort": keeperPort, "redisIp": keeperIp})

    if len(newKeepers) < 2:
        print("[%s][%s][%s]no enough new keepers "%(dc, cluster, shard) + json.dumps(newKeepers))
        return

    shardInfo['keepers'] = newKeepers
    print("[%s][%s][%s]replace keepers "%(dc, cluster, shard) + json.dumps(newKeepers))
    requests.post(XPIPE_HOST + PATH_UPDATE_SHARD%(cluster, dc, shard), json = shardInfo, headers = headers)

def fixAll():
    clusters = getUnhealthyClusters()
    for cluster in clusters:
        fixCluster(cluster)
        if limit == 0:
            return
        print("finish %s, wait a while"%cluster)
        time.sleep(INTERVAL_SECOND_BETWEEN_CLUSTER)


if __name__ == "__main__":
    refreshUnhealthyInfo()

    while True:
        action = input('''choose action:
    [1] refresh unhealthy info
    [2] show all unhealthy clusters
    [3] show cluster unhealthy shards
    [11] fix single cluster
    [12] fix n shards
    [13] fix all shards
    [21] config strict replace
    [22] config keeper port init
    [0] exit
''')

        limit = -1
        if action == 0:
            break
        elif action == 1:
            refreshUnhealthyInfo()
        elif action == 2:
            showAllUnhealthyClusters()
        elif action == 3:
            cluster = raw_input("input cluster name\n")
            showAllUnhealthyShards(cluster)
        elif action == 11:
            cluster = raw_input("input cluster name\n")
            fixCluster(cluster)
        elif action == 12:
            fixlimit = input("input fix limit\n")
            if fixlimit <= 0:
                print("fix limit should gt 0")
                continue
            limit = int(fixlimit)
            fixAll()
        elif action == 13:
            fixAll()
        elif action == 21:
            inputStrictReplace = input("1 - use strict replace; 0 - use normal strict\n")
            if inputStrictReplace == 1:
                strictReplace = True
            elif inputStrictReplace == 0:
                strictReplace = False
            else:
                print("unexpect input %"%inputStrictReplace)
            if strictReplace:
                print("now use strict replace")
            else:
                print("now use normal replace")
        elif action == 22:
            inputPortInit = input("input init port\n")
            if inputPortInit <= 0:
                print("unexpect input %d"%inputPortInit)
                continue
            KEEPER_PORT_INIT = inputPortInit
            useKeeperPorts = {}
            print("now keeper port init is %d"%KEEPER_PORT_INIT)
        # elif action == 99:
        #     strictReplaceKeepers("xpipe-test", "shard-test2", ["NTGXH"])
        else:
            print("please action id")
        print("")

