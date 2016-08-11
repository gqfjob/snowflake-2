# snowflake

Twitter's Snowflake UUID generator in Java.

This is a Java library for generating Snowflake ids:

    long workerId = 1L;
    IdWorker idWorker = new IdWorker(workerId);
    long id = idWorker.nextId();

The workerId id is a manually assigned value between 0 and 1023 which is
 used to differentiate different snowflakes when used in a multi-node cluster.

First you should install to the maven repository.

To include this as a library, you can use the following Maven dependency:

    <dependency>
        <groupId>com.github.xydonne</groupId>
        <artifactId>snowflake</artifactId>
        <version>1.0</version>
    </dependency>
