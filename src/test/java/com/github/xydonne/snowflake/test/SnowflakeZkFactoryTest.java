package com.github.xydonne.snowflake.test;

import com.github.xydonne.snowflake.SnowflakeZkFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.Timing;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Donney
 **/
public class SnowflakeZkFactoryTest extends BaseClassForTests {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final String SNOWFLAKEU_URL = "/snowflake";

    private static String appUrl = "/defaultapp";

    private static CuratorFramework client;

    @Before
    public void before() {
        try {
            super.setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Timing timing = new Timing();
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), 1, new RetryNTimes(1000, 1000));
        client.start();
        if (null == checkExists(SNOWFLAKEU_URL)) {
            createPersistent(SNOWFLAKEU_URL);
        }
        if (null == checkExists(SNOWFLAKEU_URL + appUrl)) {
            createPersistent(SNOWFLAKEU_URL + appUrl);
        }
    }

    /**
     * 测试注册节点
     */
    @Test
    public void testBuild() {

        SnowflakeZkFactory.build(server.getConnectString(), appUrl);

        assertThat("SnowflakeNode is not created!", checkExists(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(0L)), notNullValue());

        assertThat("Zookeeper Sequence Record is not equal!", Long.valueOf(new String(getData(SNOWFLAKEU_URL + appUrl))), equalTo(0L));

        assertThat("WorkerId is not equal!", SnowflakeZkFactory.getSnowflake().getWorkerId(), equalTo(0L));

        assertThat("GeneratedID is not equal!", SnowflakeZkFactory.getSnowflake().getId(), equalTo((SnowflakeZkFactory.getSnowflake().getLastTimestamp() << 22) + (SnowflakeZkFactory.getSnowflake().getWorkerId() << 12)));

        SnowflakeZkFactory.close();
    }

    /**
     * 测试权限
     */
    @Test
    public void authTest() {

        setAcl(SNOWFLAKEU_URL + appUrl, "digest", "admin:admin123");

        SnowflakeZkFactory.build(server.getConnectString(), appUrl, "admin:admin123");

        assertThat("SnowflakeNode is not created!", checkExists(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(0L)), notNullValue());

        assertThat("WorkerId is not equal!", SnowflakeZkFactory.getSnowflake().getWorkerId(), equalTo(0L));

        assertThat("GeneratedID is not equal!", SnowflakeZkFactory.getSnowflake().getId(), equalTo((SnowflakeZkFactory.getSnowflake().getLastTimestamp() << 22) + (SnowflakeZkFactory.getSnowflake().getWorkerId() << 12)));

        SnowflakeZkFactory.close();
    }

    /**
     * 测试当节点被移除,重新注册节点
     */
    @Test
    public void reRegisterTest() {

        SnowflakeZkFactory.build(server.getConnectString(), appUrl);

        assertThat("SnowflakeNode is not created!", checkExists(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(0L)), notNullValue());

        assertThat("Zookeeper Sequence Record is not equal!", Long.valueOf(new String(getData(SNOWFLAKEU_URL + appUrl))), equalTo(0L));

        assertThat("WorkerId is not equal!", SnowflakeZkFactory.getSnowflake().getWorkerId(), equalTo(0L));

        assertThat("GeneratedID is not equal!", SnowflakeZkFactory.getSnowflake().getId(), equalTo((SnowflakeZkFactory.getSnowflake().getLastTimestamp() << 22) + (SnowflakeZkFactory.getSnowflake().getWorkerId() << 12)));

        deleteNode(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(0L));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertThat("SnowflakeNode is not closed!", checkExists(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(0L)), equalTo(null));

        assertThat("SnowflakeNode is not created!", checkExists(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(1L)), notNullValue());

        assertThat("Zookeeper Sequence Record is not equal!", Long.valueOf(new String(getData(SNOWFLAKEU_URL + appUrl))), equalTo(1L));

        assertThat("WorkerId is not equal!", SnowflakeZkFactory.getSnowflake().getWorkerId(), equalTo(1L));

        assertThat("GeneratedID is not equal!", SnowflakeZkFactory.getSnowflake().getId(), equalTo((SnowflakeZkFactory.getSnowflake().getLastTimestamp() << 22) + (SnowflakeZkFactory.getSnowflake().getWorkerId() << 12)));

        SnowflakeZkFactory.close();
    }


    /**
     * 测试当appUrl下,大于等于最后创建节点记录的节点都被占用时,从0开始重新寻找可用节点
     */
    @Test
    public void cycleTest() {

        for (long i = 0L; i < 50L; i++) {
            createEphemeral(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(i));
        }
        for (long i = 1000L; i < 1024L; i++) {
            createEphemeral(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(i));
        }
        setData(SNOWFLAKEU_URL + appUrl, Long.toString(1000L).getBytes());

        SnowflakeZkFactory.build(server.getConnectString(), appUrl);

        assertThat("SnowflakeNode is not created!", checkExists(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(50L)), notNullValue());

        assertThat("Zookeeper Sequence Record is not equal!", Long.valueOf(new String(getData(SNOWFLAKEU_URL + appUrl))), equalTo(50L));

        assertThat("WorkerId is not equal!", SnowflakeZkFactory.getSnowflake().getWorkerId(), equalTo(50L));

        assertThat("GeneratedID is not equal!", SnowflakeZkFactory.getSnowflake().getId(), equalTo((SnowflakeZkFactory.getSnowflake().getLastTimestamp() << 22) + (SnowflakeZkFactory.getSnowflake().getWorkerId() << 12)));

        SnowflakeZkFactory.close();
    }

    /**
     * 测试当appUrl下节点数满,达到1024个时,不再注册并抛出异常
     */
    @Test
    public void whenFullTest() {

        for (long i = 0L; i < 1023L; i++) {
            createEphemeral(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(i));
        }

        createEphemeral(SNOWFLAKEU_URL + appUrl + "/" + String.valueOf(1023L));
        setData(SNOWFLAKEU_URL + appUrl, Long.toString(1023L).getBytes());

        exception.expect(IllegalStateException.class);
        exception.expectMessage("The snowflake node is full! The max node amount is 1024.");
        SnowflakeZkFactory.build(server.getConnectString(), appUrl);

        SnowflakeZkFactory.close();
    }

    /**
     * 测试连接是否关闭
     */
    @Test
    public void testClose() {

        SnowflakeZkFactory.build(server.getConnectString(), appUrl);

        assertThat("generatedID is not equal!", SnowflakeZkFactory.getSnowflake().getId(), equalTo((SnowflakeZkFactory.getSnowflake().getLastTimestamp() << 22) + (SnowflakeZkFactory.getSnowflake().getWorkerId() << 12)));

        SnowflakeZkFactory.close();

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Snowflake must be build first!");
        SnowflakeZkFactory.getSnowflake();

    }

    private static void createEphemeral(String path) {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static void createPersistent(String path) {
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static void setAcl(String path, String scheme, String idPassword) {
        try {
            Id id = new Id(scheme, DigestAuthenticationProvider.generateDigest(idPassword));
            ACL acl = new ACL(ZooDefs.Perms.ALL, id);
            List<ACL> acls = new ArrayList<>();
            acls.add(acl);
            client.setACL().withACL(acls).forPath(path);
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

    private static Stat setData(String path, byte[] data) {
        try {
            return client.setData().forPath(path, data);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static Void deleteNode(String path) {
        try {
            return client.delete().forPath(path);
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
