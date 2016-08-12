# snowflake

Twitter's Snowflake UUID generator in Java.

This is a Java library for generating Snowflake ids:

    long workerId = 1L;
    Snowflake snowflake = new IdWorker(workerId);
    long id = snowflake.getId();

The workerId id is a manually assigned value between 0 and 1023 which is
 used to differentiate different snowflakes when used in a multi-node cluster.

First you should install to the maven repository.

To include this as a library, you can use the following Maven dependency:

    <dependency>
        <groupId>com.github.xydonne</groupId>
        <artifactId>snowflake</artifactId>
        <version>1.0.0</version>
    </dependency>

**如何使用Quick Start**:

	1.环境安装，首先要确保机器上安装有zookeeper，同时默认zookeeper的端口为2181确保zookeeper可以连接是通的。

	2.先从仓库上clone项目，然后mvn install;

	3.接入该算法，在pom中加入依赖。

	4.使用：

	4.1 直接用类的方式：类名.build()，默认不带参数的build时zookeeper的url为本
	机的IP地址，默认端口号为2181,默认的appUrl为"/defaultapp".

	建议使用的生产环境为类名.build(String zkUrl,String appUrl);其中zkUrl为
	zookeeper的地址，appUrlapp的地址url。如下:
	SnowflakeZkController.build().getId();//测试
	SnowflakeZkController.build(zkUrl,appUrl).getId();//建议生产环境使用

	4.2 结合spring的方式，采用xml配置的方式；
	比如：xml方式
	<bean id="snowflake"
	class="com.github.xydonne.snowflake.IdWorker">
		 <constructor-arg value="2"/>
	</bean>