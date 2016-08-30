package com.github.xydonne.snowflake;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Donney
 **/
public class SnowflakeZkFactory {

    //向Zookeeper注册的根节点
    private static final String SNOWFLAKEU_URL = "/snowflake";

    //Session过期时间
    private static final int SESSION_TIMEOUT_MS = 60 * 1000;

    //连接过期时间
    private static final int CONNECTION_TIMEOUT_MS = 3000;

    //Curator客户端
    private static CuratorFramework client;

    //注册节点监听
    private static TreeCache treeCache;

    //Zookeeper连接地址
    private static String zkUrl;

    //App节点地址
    private static String appUrl;

    //授权username与Password
    private static String authority;

    //Snowflake节点ID
    private static long appWorkerID;

    //节点创建时间
    private static long pathCreatedTime;

    //是否获取远程时间并同步
    private static boolean timeSync;

    //Snowflake对象
    private static Snowflake snowflake;

    private SnowflakeZkFactory() {
    }

    //获取Snowflake对象
    public static Snowflake getSnowflake() {
        if (null != SnowflakeZkFactory.snowflake) {
            return SnowflakeZkFactory.snowflake;
        }
        throw new IllegalStateException("Snowflake must be build first!");
    }

    /**
     * 创建并获取Snowflake对象
     *
     * @param zkUrl  zookeeperURL
     * @param appUrl appName
     * @return Snowflake
     */
    public static Snowflake init(String zkUrl, String appUrl) {
        return init(zkUrl, appUrl, null, true, 0L, 1L);
    }

    /**
     * 创建并获取Snowflake对象
     *
     * @param zkUrl     zookeeperURL
     * @param appUrl    appName
     * @param authority authority
     * @return Snowflake
     */
    public static Snowflake init(String zkUrl, String appUrl, String authority) {
        return init(zkUrl, appUrl, authority, true, 0L, 1L);
    }

    /**
     * 创建并获取Snowflake对象
     *
     * @param zkUrl     zookeeperURL
     * @param appUrl    appName
     * @param authority authority
     * @param timeSync  timeSync
     * @return Snowflake
     */
    public static Snowflake init(String zkUrl, String appUrl, String authority, boolean timeSync) {
        return init(zkUrl, appUrl, authority, timeSync, 0L, 1L);
    }

    /**
     * 创建并获取Snowflake对象
     *
     * @param zkUrl               zookeeperURL
     * @param appUrl              appName
     * @param authority           authority
     * @param timeSync            timeSync
     * @param epoch               epoch
     * @param refreshTimeAfterNTP refreshTimeAfterNTP
     * @return Snowflake
     */
    public static Snowflake init(String zkUrl, String appUrl, String authority, boolean timeSync, long epoch, long refreshTimeAfterNTP) {
        if (null != SnowflakeZkFactory.snowflake) {
            return SnowflakeZkFactory.snowflake;
        }
        if (null == zkUrl || null == appUrl) {
            throw new IllegalArgumentException("zkUrl and appUrl cannot be null!");
        }
        SnowflakeZkFactory.zkUrl = zkUrl;
        SnowflakeZkFactory.appUrl = appUrl;
        SnowflakeZkFactory.authority = authority;
        SnowflakeZkFactory.timeSync = timeSync;
        initZkClient();
        doRegister();
        SnowflakeZkFactory.snowflake = new IdWorker(appWorkerID, epoch, refreshTimeAfterNTP);
        return SnowflakeZkFactory.snowflake;
    }

    /**
     * 关闭连接,清除Snowflake对象
     * 注意只是SnowflakeZkController.getSnowflake()无法获取对象,如果留有snowflake的引用,依旧能生成ID
     * 所以建议在程序中使用SnowflakeZkController.getSnowflake()
     * 而非Snowflake snowflake = SnowflakeZkFactory.getSnowflake()
     */
    public static void close() {
        SnowflakeZkFactory.treeCache.close();
        if (null != client) {
            client.close();
        }
        client = null;
        SnowflakeZkFactory.snowflake = null;
    }

    /**
     * 初始化连接,创建Snowflake根节点与App节点
     */
    private static void initZkClient() {
        //创建client
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .sessionTimeoutMs(SESSION_TIMEOUT_MS)
                .connectionTimeoutMs(CONNECTION_TIMEOUT_MS)
                .canBeReadOnly(false)
                .retryPolicy(new ExponentialBackoffRetry(1000, Integer.MAX_VALUE))
                .namespace(null)
                .defaultData(null);

        //添加权限控制
        if (authority != null && authority.length() > 0) {
            ACLProvider aclProvider = new ACLProvider() {
                private List<ACL> acls;

                @Override
                public List<ACL> getDefaultAcl() {
                    if (acls == null) {
                        ArrayList<ACL> acls = ZooDefs.Ids.CREATOR_ALL_ACL;
                        acls.clear();
                        acls.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", authority)));
                        this.acls = acls;
                    }
                    return acls;
                }

                @Override
                public List<ACL> getAclForPath(String path) {
                    return acls;
                }
            };
            builder = builder
                    .aclProvider(aclProvider)
                    .authorization("digest", authority.getBytes());
        }
        client = builder.build();

        //连接,并初始化节点数据
        client.start();
        if (null == checkExists(SNOWFLAKEU_URL)) {
            createPersistent(SNOWFLAKEU_URL);
        }
        if (null == checkExists(SNOWFLAKEU_URL + appUrl)) {
            createPersistent(SNOWFLAKEU_URL + appUrl);
        }

    }

    /**
     * 向Zookeeper的App节点获取列表,寻找空余ID节点,并注册
     */
    private static void doRegister() {
        //获取最后创建节点计数
        Long nodeNum = null;
        try {
            nodeNum = Long.valueOf(new String(getData(SNOWFLAKEU_URL + appUrl)));
        } catch (NumberFormatException ignored) {
        }
        if (null == nodeNum) {
            //如果没有创建节点,则创建节点
            appWorkerID = 0L;
            createSnowflakeNode();
        } else if (nodeNum >= 0L && nodeNum <= 1023L) {
            //如果最后创建节点计数在0-1023,最后节点+1,并创建新节点
            List<String> strWorkIDs = getChildren(SNOWFLAKEU_URL + appUrl);
            Set<Long> longWorkIDs = new HashSet<>();
            for (String each : strWorkIDs) {
                Long aLong = Long.valueOf(each);
                if (aLong != null && aLong >= 0 && aLong <= 1023) {
                    longWorkIDs.add(aLong);
                }
            }
            if (longWorkIDs.size() == 1024) {
                throw new IllegalStateException("The snowflake node is full! The max node amount is 1024.");
            }
            for (long i = nodeNum + 1; i <= 1024L; i++) {
                if (i == 1024L) {
                    i = 0L;
                }
                if (!longWorkIDs.contains(i)) {
                    appWorkerID = i;
                    createSnowflakeNode();
                    break;
                }
            }
        } else if (nodeNum < 0 || nodeNum > 1023) {
            //如果最后创建节点计数为其他数值则报错
            throw new IllegalStateException("There is something wrong with zookeeper snowflake node. The last workerID is " + nodeNum + ".");
        }
    }

    /**
     * 创建Snowflake的ID节点
     */
    private static void createSnowflakeNode() {
        final String path = SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(appWorkerID);
        try {

            //创建节点与更新节点计数
            client.inTransaction().setData().forPath(SNOWFLAKEU_URL + appUrl, Long.toString(appWorkerID).getBytes())
                    .and().create().withMode(CreateMode.EPHEMERAL).forPath(path)
                    .and().commit();

            //注册节点的监听,当节点数据发生改变时,检测节点创建时间,如果与上次节点创建时间不同,说明非此app注册的节点,需要重新创建节点
            final TreeCache treeCache = new TreeCache(client, path);
            SnowflakeZkFactory.treeCache = treeCache;
            treeCache.getListenable().addListener(
                    new TreeCacheListener() {
                        @Override
                        public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                            long pathTime;
                            try {
                                pathTime = checkExists(path).getCtime();
                            } catch (Exception e) {
                                pathTime = 0;
                            }
                            if (pathCreatedTime != pathTime) {
                                doRegister();
                                snowflake.setWorkerID(appWorkerID);
                                treeCache.close();
                            }
                        }
                    }
            );
            treeCache.start();

        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        pathCreatedTime = checkExists(path).getCtime();
        //获取节点创建时间,并以把此时间设置为当前系统时间
        if (timeSync) {
            updateSystemTime(pathCreatedTime);
            timeSync = false;
        }
    }

    /***
     * 修改当前系统的时间
     */
    private static void updateSystemTime(long date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(date);
        String strDate = calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1) + "-" + calendar.get(Calendar.DAY_OF_MONTH);
        String strTime = calendar.get(Calendar.HOUR_OF_DAY) + ":" + calendar.get(Calendar.MINUTE) + ":" + calendar.get(Calendar.SECOND);
        switch (osType()) {
            case "linux":
                try {
                    Runtime.getRuntime().exec("sudo date -s " + strDate);
                    Runtime.getRuntime().exec("sudo date -s " + strTime);
                } catch (IOException e) {
                    throw new IllegalStateException("Update system time failed!", e);
                }
                break;
            case "windows":
//                Windows系统需要获取管理员权限才能修改时间,不同语言不同版本的Windows设置时间参数的格式也不同
//                所以暂时取消Windows的时间设置功能
//                Runtime.getRuntime().exec("cmd /c date " + strDate);
//                Runtime.getRuntime().exec("cmd /c time  " + strTime);
                break;
            default:
                throw new IllegalArgumentException("Unsupported OS type!");
        }
    }

    /**
     * 返回当前操作系统类型
     */
    private static String osType() {
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            return "windows";
        }
        return "linux";
    }

    private static void createPersistent(String path) {
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static byte[] getData(String path) {
        try {
            return client.getData().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static Stat checkExists(String path) {
        try {
            return client.checkExists().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}