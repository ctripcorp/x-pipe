package com.ctrip.xpipe.redis.console.model;

/**
 * @author taotaotu
 * May 24, 2019
 */
public class ZoneModel {

    private String name;

    public String getName(){
        return this.name;
    }

    public void setName(String name){
        this.name = name;
    }


    @Override
    public String toString() {
        return "ZoneModel{" +
                "name='" + name + '\'' +
                '}';
    }
}
