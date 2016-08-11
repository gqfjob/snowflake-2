package com.github.xydonne.snowflake;

/**
 * @author Donney
 **/
public interface Snowflake {

    //获取时间起始标记点
    long getEpoch();

    //获取应用标识ID
    long getWorkerId();

    //设置应用标识ID
    void setWorkerID(long workerID);

    //获取最后更新时间
    long getLastTimestamp();

    //生成并获取Snowflake ID
    long getId();

}
