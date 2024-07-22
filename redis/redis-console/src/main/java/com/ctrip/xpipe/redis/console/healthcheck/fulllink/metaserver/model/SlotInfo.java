package com.ctrip.xpipe.redis.console.healthcheck.fulllink.metaserver.model;

import com.ctrip.xpipe.codec.JsonCodable;

public class SlotInfo extends JsonCodable implements Cloneable{

    private int serverId;
    private SLOT_STATE slotState = SLOT_STATE.NORMAL;

    private int toServerId;

    public SlotInfo(){

    }

    public SlotInfo(int serverId){
        this.serverId = serverId;
    }

    public void moveingSlot(int toServerId){

        slotState = SLOT_STATE.MOVING;
        this.toServerId = toServerId;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public SLOT_STATE getSlotState() {
        return slotState;
    }

    public int getToServerId() {
        return toServerId;
    }

    public static SlotInfo decode(byte []bytes){
        return JsonCodable.decode(bytes, SlotInfo.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("serverId:" + serverId);
        if(slotState == SLOT_STATE.MOVING){
            sb.append("->toServerId:" + toServerId);
        }
        return sb.toString();
    }

    @Override
    public SlotInfo clone() throws CloneNotSupportedException {

        SlotInfo clone = (SlotInfo) super.clone();
        return clone;
    }
}
