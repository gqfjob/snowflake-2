package com.github.xydonne.snowflake;

/**
 * Twitter的Snowflake ID算法Java实现
 * id构成: 第一位0 + 41位的时间前缀 + 10位的节点标识 + 12位的sequence避免并发的数字(12位不够用时强制得到新的时间前缀)
 * 对系统时间的依赖性非常强，当检测到ntp时间调整后，将一直获取新的时间,直至超过最后获取到的时间
 *
 * @author Donney
 */

public class IdWorker implements Snowflake {

    // 时间起始标记点，作为基准，一般取系统的最近时间(例如 EPOCH = 946656000000L 为2000年01月01日 00:00开始计时)
    private final long epoch;
    // 应用标识位数
    private static final int WORKER_ID_BITS = 10;
    // 毫秒内自增位
    private static final int SEQUENCE_BITS = 12;
    // workerId左移位数: 12
    private static final int WORKER_ID_SHIFT = SEQUENCE_BITS;
    // timestamp左移位数: 22
    private static final int TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    // 应用ID最大值: 1023
    private static final long MAX_WORKER_ID = ~(-1 << WORKER_ID_BITS);
    // 自增序列最大值: 4095
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);
    // 当检测到ntp时间调整后,再次获取时间的间隔(单位:毫秒)
    private final long refreshTimeAfterNTP;
    // 并发控制
    private long sequence = 0L;
    // 最后更新时间
    private long lastTimestamp = 0L;
    // 应用标识ID
    private long workerId;

    @Override
    public long getEpoch() {
        return epoch;
    }

    @Override
    public long getWorkerId() {
        return workerId;
    }

    @Override
    public void setWorkerID(long workerID) {
        this.workerId = workerID;
    }

    @Override
    public long getLastTimestamp() {
        return this.lastTimestamp;
    }

    @Override
    public long getId() {
        return this.nextId();
    }

    /**
     * 构造方法
     */
    public IdWorker() {
        this(0L, 0L, 1L);
    }

    /**
     * 构造方法
     *
     * @param workerId 应用标识
     */
    public IdWorker(long workerId) {
        this(workerId, 0L, 1L);
    }


    /**
     * 构造方法
     *
     * @param workerId 应用标识
     * @param epoch    时间起始标记点
     */
    public IdWorker(long workerId, long epoch) {
        this(workerId, epoch, 1L);
    }


    /**
     * 构造方法
     *
     * @param workerId    应用标识
     * @param epoch       时间起始标记点
     * @param refreshTimeAfterNTP 当检测到ntp时间调整后,再次获取时间的间隔(单位:毫秒)
     */
    public IdWorker(long workerId, long epoch, long refreshTimeAfterNTP) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(String.format("workerId can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        this.workerId = workerId;
        this.epoch = epoch;
        this.refreshTimeAfterNTP = refreshTimeAfterNTP;
    }


    /**
     * 获取Snowflake ID
     *
     * @return Snowflake ID
     */
    private synchronized long nextId() {
        long timestamp = timeGen();
        // 如果上一个timestamp与新产生的相等，则sequence加一(0-4095循环); 对新的timestamp，sequence从0开始
        if (timestamp == this.lastTimestamp) {
            this.sequence = this.sequence + 1L & MAX_SEQUENCE;
            if (this.sequence == 0L) {
                timestamp = this.tilNextMillis(this.lastTimestamp);
            }
        } else {
            if (timestamp < this.lastTimestamp) {
                timestamp = afterNTP(this.lastTimestamp);
            }
            this.sequence = 0L;
        }
        this.lastTimestamp = timestamp;
        // 返回Snowflake ID: 第一位0 + 41位的时间前缀 + 10位的节点标识 + 12位的sequence避免并发的数字(12位不够用时强制得到新的时间前缀)
        return timestamp - epoch << TIMESTAMP_SHIFT | this.workerId << WORKER_ID_SHIFT | this.sequence;
    }

    private static long timeGen() {
        return System.currentTimeMillis();
    }

    /**
     * 当sequence为0时,等待到下一秒再生成ID
     *
     * @return timestamp
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp == lastTimestamp) {
            timestamp = timeGen();
        }
        if (timestamp < lastTimestamp) {
            timestamp = afterNTP(this.lastTimestamp);
        }
        return timestamp;
    }

    /**
     * 当NTP时间调整后,导致时间回溯时,将一直获取新的时间,直至超过最后获取到的时间
     *
     * @return timestamp
     */
    private long afterNTP(long lastTimestamp) {
        // 当检测到ntp时间调整后，将一直获取新的时间,直至超过最后获取到的时间
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            // 默认每1毫秒检测一次时间,防止CPU满载
            try {
                Thread.sleep(refreshTimeAfterNTP);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            timestamp = timeGen();
        }
        return timestamp;
    }

}