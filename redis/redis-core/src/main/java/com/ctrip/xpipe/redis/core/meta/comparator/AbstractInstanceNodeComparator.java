package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractInstanceNodeComparator extends  AbstractMetaComparator<InstanceNode>{


    protected void compareConfigConfig(List<Pair<InstanceNode, InstanceNode>> allModified) {

        for(Pair<InstanceNode, InstanceNode> pair : allModified){
            InstanceNode current = pair.getValue();
            InstanceNode future = pair.getKey();
            if(current.equals(future)){
                continue;
            }
            InstanceNodeComparator instanceNodeComparator = new InstanceNodeComparator(current, future);
            instanceNodeComparator.compare();
            modified.add(instanceNodeComparator);
        }

    }

    protected Pair<List<InstanceNode>, List<Pair<InstanceNode, InstanceNode>>> sub(List<InstanceNode> all1, List<InstanceNode> all2) {

        List<InstanceNode> subResult = new LinkedList<>();
        List<Pair<InstanceNode, InstanceNode>> intersectResult = new LinkedList<>();

        for(InstanceNode instanceNode1 : all1){

            InstanceNode instanceNode2Equal = null;
            for(InstanceNode instanceNode2 : all2){
                if(instanceNodeTheSame(instanceNode1, instanceNode2)){
                    instanceNode2Equal = instanceNode2;
                    break;
                }
            }
            if(instanceNode2Equal == null){
                subResult.add(instanceNode1);
            }else{
                intersectResult.add(new Pair<>(instanceNode1, instanceNode2Equal));
            }
        }
        return new Pair<List<InstanceNode>, List<Pair<InstanceNode, InstanceNode>>>(subResult, intersectResult);
    }

    private boolean instanceNodeTheSame(InstanceNode instanceNode1, InstanceNode instanceNode2) {
        if (!MetaUtils.theSame(instanceNode1, instanceNode2)) {
            return false;
        }
        if (instanceNode1 instanceof ApplierMeta && instanceNode2 instanceof ApplierMeta) {
            return Objects.equals(((ApplierMeta) instanceNode1).getTargetClusterName(),
                    ((ApplierMeta) instanceNode2).getTargetClusterName());
        }
        return true;
    }
}
