# Janusgraph编程指南

## 介绍

JanusGraph是可扩展的图形数据库，已优化用于存储和查询包含分布在多计算机集群中的数千亿个顶点和边的图形。JanusGraph是Linux基金会下的一个项目，参与者来自Expero，Google，GRAKN.AI，Hortonworks，IBM和Amazon。

- Scalable -弹性和线性可扩展性，可满足不断增长的数据和用户群的需求；数据分发和复制，以提高性能和容错能力；多数据中心高可用性和热备份
-   Open source - 所有功能都是完全免费的。 无需购买商业许可证。 JanusGraph是Apache 2许可下的完全开放源代码。
- Transactional - JanusGraph是一个事务数据库，可以支持数千个并发用户实时执行复杂的图形遍历。 支持ACID和最终的一致性。
- Data Storage - Graph data可以存储在[Apache Cassandra](https://cassandra.apache.org/)、[Apache HBase](https://hbase.apache.org/)、[Google Cloud Bigtable](https://cloud.google.com/bigtable)、[Oracle BerkeleyDB](https://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html)、[ScyllaDB](https://scylladb.com/)等
- Search - 全文搜索等高级搜索功能可以选择通过以下方式支持：[Elasticsearch](https://www.elastic.co/)、[Apache Solr](https://lucene.apache.org/solr)、[Apache Lucene](https://lucene.apache.org/)

- Analytics -  除了在线事务处理（OLTP）之外，JanusGraph的Apache Spark集成还支持全局图分析（OLAP）。

-  TinkerPop - 与Apache TinkerPop™图形堆栈的本机集成：[Gremlin graph query language](https://tinkerpop.apache.org/docs/current/reference/#intro)、[Gremlin Server](https://tinkerpop.apache.org/docs/current/reference/#gremlin-server)、[Gremlin Console](https://tinkerpop.apache.org/docs/current/reference/#gremlin-console)
- Adapters - JanusGraph有不同的第三方存储适配器：:[Aerospike](https://github.com/Playtika/aerospike-janusgraph-storage-backend)、[DynamoDB](https://github.com/awslabs/dynamodb-janusgraph-storage-backend)、[FoundationDB](https://github.com/experoinc/janusgraph-foundationdb)
- Visualization - JanusGraph支持各种可视化工具，例如Arcade Analytics，Cytoscape，Apache TinkerPop的Gephi插件，Graphexp，Cambridge Intelligence的Key Lines，Linkurious和Tom Sawyer Perspectives

## 安装

### 部署Janusgraph

①安装jdk，安装HBase、安装elasticsearch-6.3.0，由于前面很多文章讲解关于HBase安装环境，这里就不在赘述了。这里写一下Elasticsearch的安装步骤。

```PowerShell
[root@CentOS ~]# rpm -ivh jdk-8u271-linux-x64.rpm
[root@CentOS ~]# groupadd es
 [root@CentOS ~]# useradd es -g es
[root@CentOS ~]# passwd es
Changing password for user es.
New password:
[root@CentOS ~]# tail -n 10  /etc/security/limits.conf
#@faculty        soft    nproc           20
#@faculty        hard    nproc           50
#ftp             hard    nproc           0
#@student        -       maxlogins       4

# End of file
* soft nofile 204800
* hard nofile 204800
* soft nproc 204800
* hard nproc 204800
[root@CentOS ~]# tail -n 10  /etc/sysctl.conf
# /usr/lib/sysctl.d/, /run/sysctl.d/, and /etc/sysctl.d/.
#
# Vendors settings live in /usr/lib/sysctl.d/.
# To override a whole file, create a new file with the same in
# /etc/sysctl.d/ and put new settings there. To override
# only specific settings, add a file with a lexically later
# name in /etc/sysctl.d/ and put new settings there.
#
# For more information, see sysctl.conf(5) and sysctl.d(5).
vm.max_map_count=262144
[root@CentOS nginx-1.16.1]# su es
[es@CentOS nginx-1.16.1]$ cd
[es@CentOS elasticsearch-6.3.0]$ ./bin/elasticsearch -daemon
```

需要注意这里需要重启系统或者退出当前用户重新登录才能生效！

附：环境变量信息如下

```PowerShell
JAVA_HOME=/usr/java/latest
HIVE_HOME=/usr/apache-hive-2.3.3-bin
HADOOP_HOME=/usr/hadoop-2.7.6
HBASE_HOME=/usr/hbase-2.2.4
CLASSPATH=.
PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$HBASE_HOME/bin
export JAVA_HOME
export CLASSPATH
export PATH
export HADOOP_HOME
export HIVE_AUX_JARS_PATH=/root/atlas-hive-hook-2.1.0_2.3.3
export HIVE_HOME
export HBASE_HOME
```

②安装[janusgraph-0.5.3.zip](https://github-production-release-asset-2e65be.s3.amazonaws.com/77385607/1ed3f200-46ba-11eb-9574-4dbc17d45123?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIWNJYAX4CSVEH53A/20210125/us-east-1/s3/aws4_request&X-Amz-Date=20210125T031615Z&X-Amz-Expires=300&X-Amz-Signature=ad0f1445ddf9ded94d61188e7a1938b1359021eeee5cb1c6c9f3e41fab458a19&X-Amz-SignedHeaders=host&actor_id=7834990&key_id=0&repo_id=77385607&response-content-disposition=attachment; filename=janusgraph-0.5.3.zip&response-content-type=application/octet-stream)文件，下载地址https://github.com/JanusGraph/janusgraph/releases这里我们下载的是0.5.3版本

1、解压该文件

```PowerShell
[root@CentOS ~]# yum install -y unzip
[root@CentOS ~]# unzip janusgraph-0.5.3.zip
```

2、修改配置文件

- 编辑gremlin-server.yaml

```PowerShell
[root@CentOS janusgraph-0.5.3]# vi conf/gremlin-server/gremlin-server.yaml
host: 0.0.0.0
port: 8182
scriptEvaluationTimeout: 30000
channelizer: org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer
graphs: {
  graph: conf/janusgraph-hbase-es.properties
}
```

- 编辑janusgraph-hbase-es.properties

```PowerShell
[root@CentOS janusgraph-0.5.3]# vi conf/janusgraph-hbase-es.properties
# Copyright 2019 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# JanusGraph configuration sample: HBase and Elasticsearch
#
# This file connects to HBase using a Zookeeper quorum
# (storage.hostname) consisting solely of localhost.  It also connects
# to Elasticsearch running on localhost over Elasticsearch's native "Transport"
# protocol.  Zookeeper, the HBase services, and Elasticsearch must already 
# be running and available before starting JanusGraph with this file.

# The implementation of graph factory that will be used by gremlin server
#
# Default:    org.janusgraph.core.JanusGraphFactory
# Data Type:  String
# Mutability: LOCAL
gremlin.graph=org.janusgraph.core.JanusGraphFactory

# The primary persistence provider used by JanusGraph.  This is required. 
# It should be set one of JanusGraph's built-in shorthand names for its
# standard storage backends (shorthands: berkeleyje, cassandrathrift,
# cassandra, astyanax, embeddedcassandra, cql, hbase, inmemory) or to the
# full package and classname of a custom/third-party StoreManager
# implementation.
#
# Default:    (no default value)
# Data Type:  String
# Mutability: LOCAL
storage.backend=hbase

# The hostname or comma-separated list of hostnames of storage backend
# servers.  This is only applicable to some storage backends, such as
# cassandra and hbase.
#
# Default:    127.0.0.1
# Data Type:  class java.lang.String[]
# Mutability: LOCAL
storage.hostname=CentOS

# Whether to enable JanusGraph's database-level cache, which is shared
# across all transactions. Enabling this option speeds up traversals by
# holding hot graph elements in memory, but also increases the likelihood
# of reading stale data.  Disabling it forces each transaction to
# independently fetch graph elements from storage before reading/writing
# them.
#
# Default:    false
# Data Type:  Boolean
# Mutability: MASKABLE
cache.db-cache = true

# How long, in milliseconds, database-level cache will keep entries after
# flushing them.  This option is only useful on distributed storage
# backends that are capable of acknowledging writes without necessarily
# making them immediately visible.
#
# Default:    50
# Data Type:  Integer
# Mutability: GLOBAL_OFFLINE
#
# Settings with mutability GLOBAL_OFFLINE are centrally managed in
# JanusGraph's storage backend.  After starting the database for the first
# time, this file's copy of this setting is ignored.  Use JanusGraph's
# Management System to read or modify this value after bootstrapping.
cache.db-cache-clean-wait = 20

# Default expiration time, in milliseconds, for entries in the
# database-level cache. Entries are evicted when they reach this age even
# if the cache has room to spare. Set to 0 to disable expiration (cache
# entries live forever or until memory pressure triggers eviction when set
# to 0).
#
# Default:    10000
# Data Type:  Long
# Mutability: GLOBAL_OFFLINE
#
# Settings with mutability GLOBAL_OFFLINE are centrally managed in
# JanusGraph's storage backend.  After starting the database for the first
# time, this file's copy of this setting is ignored.  Use JanusGraph's
# Management System to read or modify this value after bootstrapping.
cache.db-cache-time = 180000

# Size of JanusGraph's database level cache.  Values between 0 and 1 are
# interpreted as a percentage of VM heap, while larger values are
# interpreted as an absolute size in bytes.
#
# Default:    0.3
# Data Type:  Double
# Mutability: MASKABLE
cache.db-cache-size = 0.5

# The indexing backend used to extend and optimize JanusGraph's query
# functionality. This setting is optional.  JanusGraph can use multiple
# heterogeneous index backends.  Hence, this option can appear more than
# once, so long as the user-defined name between "index" and "backend" is
# unique among appearances.Similar to the storage backend, this should be
# set to one of JanusGraph's built-in shorthand names for its standard
# index backends (shorthands: lucene, elasticsearch, es, solr) or to the
# full package and classname of a custom/third-party IndexProvider
# implementation.
#
# Default:    elasticsearch
# Data Type:  String
# Mutability: GLOBAL_OFFLINE
#
# Settings with mutability GLOBAL_OFFLINE are centrally managed in
# JanusGraph's storage backend.  After starting the database for the first
# time, this file's copy of this setting is ignored.  Use JanusGraph's
# Management System to read or modify this value after bootstrapping.
index.search.backend=elasticsearch

# The hostname or comma-separated list of hostnames of index backend
# servers.  This is only applicable to some index backends, such as
# elasticsearch and solr.
#
# Default:    127.0.0.1
# Data Type:  class java.lang.String[]
# Mutability: MASKABLE
index.search.hostname=CentOS
```

- 启动janusgraph服务

```PowerShell
[root@CentOS janusgraph-0.5.3]# ./bin/gremlin-server.sh start conf/gremlin-server/gremlin-server.yaml
Server started 52963.
```

查看更多帮助信息，可以键入如下命令：[gremlin-server.sh](http://gremlin-server.sh) help命令

- 链接janusgraph服务

```PowerShell
[root@CentOS janusgraph-0.5.3]# vi conf/remote.yaml
# Copyright 2019 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

hosts: [CentOS]
port: 8182
serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0, config: { serializeResultToString: true }}
```

- 加载测试数据

由于这里使用的是hbase-es存储和索引的后端，因此这里需要加载janusgraph-hbase-es.properties配置文件

```PowerShell
[root@CentOS janusgraph-0.5.3]# ./bin/gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
plugin activated: tinkerpop.server
plugin activated: tinkerpop.tinkergraph
15:21:49 WARN  org.apache.hadoop.util.NativeCodeLoader  - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
plugin activated: tinkerpop.hadoop
plugin activated: tinkerpop.spark
plugin activated: tinkerpop.utilities
plugin activated: janusgraph.imports
gremlin> graph = JanusGraphFactory.open('conf/janusgraph-hbase-es.properties')
==>standardjanusgraph[hbase:[CentOS]]
gremlin> GraphOfTheGodsFactory.load(graph)
==>null
gremlin> g = graph.traversal()
==>graphtraversalsource[standardjanusgraph[hbase:[CentOS]], standard]
```

### 安装[graphexp](https://github.com/bricaud/graphexp)

①安装nginx服务

1.安装nginx服务

```PowerShell
[root@CentOS ~]#  yum install -y gcc-c++ pcre-devel zlib-devel wget 
[root@CentOS ~]#  wget http://nginx.org/download/nginx-1.16.1.tar.gz
[root@CentOS ~]# tar -zxf nginx-1.16.1.tar.gz
[root@CentOS nginx-1.16.1]# ./configure --prefix=/usr/local/nginx-1.16.1/
[root@CentOS nginx-1.16.1]# make && make install
```

2.修改nginx.conf文件

```.properties
user  root;
worker_processes  2;

#error_log  logs/error.log;
#error_log  logs/error.log  notice;
#error_log  logs/error.log  info;

#pid        logs/nginx.pid;


events {
    worker_connections  1024;
}


http {
    include       mime.types;
    default_type  application/octet-stream;

    #log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
    #                  '$status $body_bytes_sent "$http_referer" '
    #                  '"$http_user_agent" "$http_x_forwarded_for"';

    #access_log  logs/access.log  main;

    sendfile        on;
    #tcp_nopush     on;

    #keepalive_timeout  0;
    keepalive_timeout  65;

    #gzip  on;

    server {
        listen       80;
        server_name  CentOS;
        
        #charset koi8-r;

        #access_log  logs/host.access.log  main;

        location / {
        # 配置的基准目录
            root   /root/graphexp;
            index  index.html index.htm;
        }

        #error_page  404              /404.html;

        # redirect server error pages to the static page /50x.html
        #
        error_page   500 502 503 504  /50x.html;
        location = /50x.html {
            root   html;
        }

        # proxy the PHP scripts to Apache listening on 127.0.0.1:80
        #
        #location ~ \.php$ {
        #    proxy_pass   http://127.0.0.1;
        #}

        # pass the PHP scripts to FastCGI server listening on 127.0.0.1:9000
        #
        #location ~ \.php$ {
        #    root           html;
        #    fastcgi_pass   127.0.0.1:9000;
        #    fastcgi_index  index.php;
        #    fastcgi_param  SCRIPT_FILENAME  /scripts$fastcgi_script_name;
        #    include        fastcgi_params;
        #}

        # deny access to .htaccess files, if Apache's document root
        # concurs with nginx's one
        #
        #location ~ /\.ht {
        #    deny  all;
        #}
    }


    # another virtual host using mix of IP-, name-, and port-based configuration
    #
    #server {
    #    listen       8000;
    #    listen       somename:8080;
    #    server_name  somename  alias  another.alias;

    #    location / {
    #        root   html;
    #        index  index.html index.htm;
    #    }
    #}


    # HTTPS server
    #
    #server {
    #    listen       443 ssl;
    #    server_name  localhost;

    #    ssl_certificate      cert.pem;
    #    ssl_certificate_key  cert.key;

    #    ssl_session_cache    shared:SSL:1m;
    #    ssl_session_timeout  5m;

    #    ssl_ciphers  HIGH:!aNULL:!MD5;
    #    ssl_prefer_server_ciphers  on;

    #    location / {
    #        root   html;
    #        index  index.html index.htm;
    #    }
    #}

}
```

②下载[graphexp](https://github.com/bricaud/graphexp)文件

```PowerShell
[root@CentOS ~]# git clone https://github.com/bricaud/graphexp.git
[root@CentOS ~]# cd graphexp/
[root@CentOS graphexp]# pwd
/root/graphexp
[root@CentOS graphexp]# tree -L 1
.
├── css
├── graphexp.html
├── images
├── index.html
├── LICENSE
├── README.md
└── scripts

3 directories, 4 files
```

- 启动nginx服务

```XML
[root@CentOS nginx-1.16.1]# ./sbin/nginx -t
nginx: the configuration file /usr/local/nginx-1.16.1/conf/nginx.conf syntax is ok
nginx: configuration file /usr/local/nginx-1.16.1/conf/nginx.conf test is successful
[root@CentOS nginx-1.16.1]# ./sbin/nginx
```

![image-20210324104947478](assets/image-20210324104947478.png)

## Java API Intergation

①导入maven依赖

```XML
<!-- https://mvnrepository.com/artifact/org.janusgraph/janusgraph-core -->
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-core</artifactId>
    <version>0.5.3</version>
    <exclusions>
        <exclusion>
            <artifactId>commons-logging</artifactId>
            <groupId>commons-logging</groupId>
        </exclusion>
    </exclusions>
</dependency>

<!-- https://mvnrepository.com/artifact/org.janusgraph/janusgraph-hbase -->
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-hbase</artifactId>
    <version>0.5.3</version>
</dependency>

<!-- https://mvnrepository.com/artifact/org.janusgraph/janusgraph-solr -->
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-es</artifactId>
    <version>0.5.3</version>
    <exclusions>
        <exclusion>
            <artifactId>logback-classic</artifactId>
            <groupId>ch.qos.logback</groupId>
        </exclusion>
    </exclusions>
</dependency>
<!-- https://mvnrepository.com/artifact/org.janusgraph/janusgraph-driver -->
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-driver</artifactId>
    <version>0.5.3</version>
</dependency>

<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-client</artifactId>
    <version>2.0.2</version>
    <exclusions>
        <exclusion>
            <artifactId>commons-logging</artifactId>
            <groupId>commons-logging</groupId>
        </exclusion>
    </exclusions>
</dependency>

<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.12</version>
    <scope>test</scope>
</dependency>
```

②引入log4j.proeprties

```properties
### 设置###
log4j.rootLogger = ERROR,stdout

log4j.logger.com.jiangzz=debug
### 输出信息到控制抬 ###
log4j.appender.stdout = org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target = System.out
log4j.appender.stdout.layout = org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern = [%p] %d{yyyy-MM-dd HH:mm:ss} %c - %m%n
```

## Schema & Data Modeling

每个JanusGraph图具有一个schema ，该schema 包括其中使用的edge labels, property keys和vertex labels。 可以显式或隐式定义JanusGraph schema 。 鼓励用户在应用程序开发期间明确定义JanusGraph Schema。 显式定义的架构是鲁棒图应用程序的重要组成部分，可以极大地改善协作软件的开发。 请注意，JanusGraph Schema可以随着时间的推移而演变，而不会中断正常的数据库操作。 扩展架构不会减慢查询的响应速度，也不需要数据库停机。

首次创建时，将schema type（edge labels, property keys和vertex labels）分配给图形中的元素-即分别为edge，property 或vertex 。 不能为特定元素更改分配的schema type。 这确保了易于推理的稳定类型系统。

### Schema信息

有一些方法可以在管理API中查看图形Schema的特定元素。 这些方法是mgmt.printIndexes（），mgmt.printPropertyKeys（），mgmt.printVertexLabels（）和mgmt.printEdgeLabels（）。 还有一种方法可以显示所有名为printSchema（）的组合输出。

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//边Label信息
String edgeLabels = mgmt.printEdgeLabels();
//顶点Label信息
String vertexLabels = mgmt.printVertexLabels();
//属性信息
String propertyKeys = mgmt.printPropertyKeys();
//索引信息
String indexes = mgmt.printIndexes();

//全量信息，涵盖edge、vertex、property、index等
String schema = mgmt.printSchema();
```

附录：

```java
JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.hostname", "CentOS")
                .set("storage.backend", "hbase")
                .set("storage.hbase.table", "janusgraph")
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", "CentOS");
JanusGraph janusGraph = builder.open();
//TODO LIST
janusGraph.close();
```

###  Edge Labels

连接两个顶点的每个边都有一个标签，用于定义关系的语义。 例如，顶点A和顶点B之间标记为`friend`的edge编码了两个人之间的友谊。

要定义边缘标签，请在打开的janusGraph或mgmt 事务上调用`makeEdgeLabel（String）`并提供边缘标签的名称作为参数。 边缘标签名称在图中必须唯一。 此方法返回允许定义其多重性的边标签生成器。 边缘标签的多重性定义了该标签所有边缘上的多重性约束，即，成对的顶点之间的最大边缘数。 JanusGraph可以识别以下多重设置。

- Edge Label Multiplicity

  - MULTI - 允许在任意一对顶点之间使用同一标签的多个边。 换句话说，该图是关于这种边缘标签的多重图。 边缘多重性没有限制。

  - SIMPLE - 在任何一对顶点之间最多允许该标签的一个边缘。 换句话说，该图是关于标签的简单图。 确保给定标签和成对的顶点的边是唯一的。
  - MANY2ONE - 在图形的任何顶点上最多允许该标签的一个输出边缘，但对输入边缘不施加任何限制。 边缘标签母亲是MANY2ONE多重性的一个例子，因为每个人最多有一个母亲，但是母亲可以有多个孩子。
  - ONE2MANY - 在图形的任何顶点上最多允许该标签的一个输入边缘，但对输出边缘不施加任何限制。 边缘标签winnerOf是一个具有ONE2MANY多重性的示例，因为每个竞赛最多只能由一个人赢得，但是一个人可以赢得多个竞赛。
  - ONE2ONE - 在图形的任何顶点上最多允许该标签的一个输入边缘和一个输出边缘。 因为一个人与另一个人结婚，所以边缘标签“marriedTo ”是ONE2ONE多重性的一个示例。

![image-20210324133125421](assets/image-20210324133125421.png)

这里需要区分SIMPLE和ONE2ONE的区别，如上图所示，SIMPLE强调的是在任意两个顶点之间关系有且仅有一条唯一的关系，例如老师和多个学生之间就存在着`师生`关系，因此这种关系一旦在两个顶点建立，不予许出现重复；ONE2ONE强调的是仅仅允许该边和图中的一个顶点产生关系，不允许和其他节点产生类似关系，例如上图中一个女士仅仅只能存在一个`丈夫`关系属性，不可以和其他人建立该关系；

默认的多重性是MULTI。 边缘标签的定义是通过在构建器上调用make（）方法完成的，该方法返回定义的边缘标签，如以下示例所示。

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//默认开启事物
mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
mgmt.makeEdgeLabel("husband").multiplicity(Multiplicity.ONE2ONE).make();
mgmt.commit();
```

### Property Key

顶点和边上的属性是键值对。 例如，属性`name='Daniel'`具有`name`键和以及值`Daniel` .  Property key是JanusGraph Schema的一部分，可以限制允许的数据类型和值的基数。

要定义Property key，请在打开的janusGraph或mgmt 事务上调用makePropertyKey（String）并提供属性键的名称作为参数。 属性键名称在图中必须是唯一的，建议避免在属性名称中使用空格或特殊字符。 此方法返回属性键的生成器。

**Note**：在创建属性键时，请考虑也创建图形索引以获得更好的性能，请参阅[Index Performance](https://docs.janusgraph.org/index-management/index-performance/).

使用dataType（Class）定义属性键的数据类型。 JanusGraph将强制与键关联的所有值都具有配置的数据类型，从而确保添加到图的数据有效。 例如，可以定义名称键具有String数据类型。 请注意，不支持primitive 类型。 使用相应的包装器类，例如Integer而不是int .

将数据类型定义为Object.class，以允许任何（可序列化的）值与键相关联。 但是，建议尽可能使用具体的数据类型。 配置的数据类型必须是具体的类，而不是接口或抽象类。 JanusGraph强制执行类相等性，因此不允许添加已配置数据类型的子类。

| Name      | Description                                      |
| :-------- | :----------------------------------------------- |
| String    | Character sequence                               |
| Character | Individual character                             |
| Boolean   | true or false                                    |
| Byte      | byte value                                       |
| Short     | short value                                      |
| Integer   | integer value                                    |
| Long      | long value                                       |
| Float     | 4 byte floating point number                     |
| Double    | 8 byte floating point number                     |
| Date      | Specific instant in time (`java.util.Date`)      |
| Geoshape  | Geographic shape like point, circle or box       |
| UUID      | Universally unique identifier (`java.util.UUID`) |

使用基数（Cardinality）定义与任何给定顶点上的键关联的值的允许基数。

- Property Key Cardinality
  - SINGLE - 此键的每个元素最多允许一个值。 换句话说，键→值映射对于图形中的所有元素都是唯一的。 属性birthDate是单基数的示例，因为每个人都有一个确切的生日。
  - LIST - 允许该键的每个元素具有任意数量的值。 换句话说，键与允许重复值的值列表相关联。 假设我们将传感器建模为图形中的顶点，则属性键sensorVals是一个具有LIST基数的示例，可以记录很多（可能重复的）传感器读数。
  - SET - 此类键的每个元素允许多个值，但不允许重复值。 换句话说，键与一组值相关联。 如果我们要捕获个人的所有name（包括昵称，姓等），则属性键名称具有SET基数。

默认基数设置为SINGLE。 请注意，在边缘和属性上使用的属性键的基数为SINGLE。 不支持在边缘或属性上为单个键附加多个值。

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//默认开启事物
mgmt.makePropertyKey("birthDate").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
mgmt.makePropertyKey("sensorVals").dataType(Double.class).cardinality(Cardinality.LIST).make();
mgmt.commit();
```

### Relation Types

边缘标签和属性键共同称为关系类型。 关系类型的名称在图中必须唯一，这意味着属性键和边标签不能具有相同的名称。 JanusGraph API中提供了一些方法来查询是否存在或检索关系类型，这些类型同时包含属性键和边标签。

```java
JanusGraphManagement mgmt = janusGraph.openManagement();

if (mgmt.containsRelationType("name")) {
    PropertyKey name = mgmt.getPropertyKey("name");
    LOGGER.info("label {} name {}",name.label(),name.name());
}
Iterable<EdgeLabel> relationTypes = mgmt.getRelationTypes(EdgeLabel.class);
for (EdgeLabel relationType : relationTypes) {
    LOGGER.info("label {} name {}",relationType.label(),relationType.name());
}
mgmt.commit();
```

### Vertex Labels

像Edges一样，Vertex 也有Label。 与EdgeLabel不同，Vertex Label是可选的。 Vertex Label可用于区分不同类型的顶点，例如 用户顶点和产品顶点。

尽管Label在概念和数据模型级别是可选的，但JanusGraph会为所有顶点分配标签作为内部实现细节。 由addVertex方法创建的顶点使用JanusGraph的默认标签。

要创建Label，请在打开的图形或管理事务上调用makeVertexLabel(String).make()并提供Vertex Label的名称作为参数。 Vertex Label名称在图中必须唯一。

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//创建一个person Label
VertexLabel personLabel = mgmt.makeVertexLabel("person").make();
mgmt.commit();
```

```java
personLabel = janusGraph.getVertexLabel("person");
//创建一个Person Label 顶点
JanusGraphVertex personVertex = janusGraph.addVertex(T.label,personLabel,"name","jiangzz");
//默认顶点类型
JanusGraphVertex nonLabelVertex = janusGraph.addVertex();
janusGraph.tx().commit();
```

如果未明确定义 edge label, property key或者vertex label ，则在添加边，顶点或属性设置期间首次使用时将隐式定义。 为JanusGraph图配置的DefaultSchemaMaker定义了此类类型。

默认情况下，隐式创建的Edge Label具有多重性MULTI，而隐式创建的Property Key具有基数SINGLE和数据类型Object.class。 用户可以通过实现和注册自己的DefaultSchemaMaker来控制自动模式元素的创建.

强烈建议通过在JanusGraph图形配置中设置schema.default = none来显式定义所有架构元素并禁用自动架构创建。

Edge Label，Property Key或Vertex Label的定义一旦提交到图形中就无法更改。 但是，可以通过JanusGraphManagement.changeName(JanusGraphSchemaElement，String)更改架构元素的名称，如以下示例所示，其中将属性键位置重命名为location。

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
PropertyKey birthDate = mgmt.getPropertyKey("birthDate");
mgmt.changeName(birthDate,"birthDay");
mgmt.commit();
```

请注意，架构名称更改在群集中当前正在运行的事务和其他JanusGraph图形实例中可能不会立即可见。虽然通过存储后端向所有JanusGraph实例宣布了架构名称更改，但是架构更改可能需要一段时间才能生效，并且在某些故障情况（例如网络分区）发生的情况下，可能需要重启实例，如果它们与重命名。因此，用户必须确保以下任一条件成立：

- 重命名的Label或Property Key当前未处于活动状态（即已写入或读取），并且直到所有JanusGraph实例都知道名称更改后才会使用。
- 正在运行的事务会主动适应短暂的中间时间段，根据特定的JanusGraph实例和名称更改公告的状态，旧名称或新名称均有效。 例如，这可能意味着事务同时查询两个名称。

如果需要重新定义现有的架构类型，建议将这种类型的名称更改为当前（永远不会）使用的名称。 之后，可以使用原始名称定义新的Label或PropertyKey，从而有效地替换旧的标签或密钥。 但是，请注意，这不会影响以前使用现有类型编写的Vertex，Edge或Property。 在线不支持重新定义现有图形元素，必须通过批处理图形转换来完成。

### Schema Constraints

模式的定义允许用户配置显式属性和连接约束。 可以将属性绑定到特定的Vertex Label或Edge Label。 此外，Connect Constraints允许用户显式定义可以通过边缘标签连接的Vertex Label。 这些约束可用于确保图与给定的域模型匹配。 例如，对于`god`的graph，一个`god`可以是另一个`god`的brother，但不能是`monster `的兄弟，一个`god`可以有age属性，但是`location`不能有age属性。 这些约束默认情况下处于禁用状态。

通过设置`schema.constraints = true`启用这些架构约束。此设置取决于设置`schema.default`。如果配置`schema.default`设置为none，则将因Schema Constraints冲突而抛出IllegalArgumentException。如果未将schema.default设置为none，则将自动创建架构约束，但不会引发任何异常。激活Schema Constraints对现有数据没有影响，因为这些Schema Constraints仅在插入过程中应用。因此，这些约束根本不会影响数据的读取。

可以使用JanusGraphManagement.addProperties（VertexLabel，PropertyKey ...）将多个属性绑定到一个顶点，例如：

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//获取现有属性定义
PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
PropertyKey name = mgmt.getPropertyKey("name");
//获取顶点标签
VertexLabel personLabel = mgmt.getVertexLabel("person");
//绑定标签属性
mgmt.addProperties(personLabel,birthDay,name);
mgmt.commit();
```

同时可以使用JanusGraphManagement.addProperties（EdgeLabel，PropertyKey ...）将多个属性绑定到一条边，例如：

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//获取现有属性定义
PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
//获取顶点标签
EdgeLabel mother = mgmt.getEdgeLabel("mother");
//绑定标签属性
mgmt.addProperties(mother,birthDay);
mgmt.commit();
```

可以使用JanusGraphManagement.addConnection（EdgeLabel，VertexLabel输出，VertexLabel输入）在传出，传入和边缘之间定义连接，例如：

```java
JanusGraphManagement mgmt = janusGraph.openManagement();
//创建 person 定点 label
VertexLabel personLabel = mgmt.getVertexLabel("person");
//创建 mother 边   label
EdgeLabel motherEdgeLabel = mgmt.getEdgeLabel("mother");
//将 使用mother的边关系连接两个person节点信息
mgmt.addConnection(motherEdgeLabel,personLabel,personLabel);
mgmt.commit();
```

**Note**：需要在`janusgraph-hbase-es.properties`添加如下配置，重启服务器！

```properties
schema.constraints=true
schema.default=none
```

## Advanced Schema

### Static Vertices

顶点标签可以定义为静态，这意味着带有该标签的顶点不能在创建它们的事务之外进行修改。Static vertex labels 

是一方法用于控制数据的生命周期，在加载数据到graph之后 ，使用定点的标签数据不允许修改。例如创建如下VertexLabel 标签：

```java
JanusGraphManagement mgmt = janusGraph.openManagement();

VertexLabel tweet = mgmt.makeVertexLabel("tweet").setStatic().make();

PropertyKey birthDay = mgmt.getPropertyKey("birthDay");
PropertyKey sensorReading = mgmt.getPropertyKey("sensorReading");
//绑定属性信息
mgmt.addProperties(tweet,birthDay,sensorReading);
mgmt.commit();

```

创建该Label的定点,并且提交事务

```java
janusGraph.addVertex(T.label, "tweet", "birthDay", System.currentTimeMillis());
janusGraph.tx().commit();
```

然后尝试更新该定点数据

```java
GraphTraversalSource traversal = janusGraph.traversal();
//查询为tweet的定点数据
Vertex tweet = traversal.V().has(T.label, "tweet").next();
//尝试修改该定点属性
tweet.property("birthDay",System.currentTimeMillis());
janusGraph.tx().commit();
```

抛出：`org.janusgraph.core.SchemaViolationException: Cannot modify unmodifiable vertex`信息

### Edge  & Vertex TTL

可以使用生存时间（TTL）来配置Edge和Vertex标签。初始创建后，已配置的TTL经过时，带有此类标签的边和顶点将自动从图形中删除。当将大量数据仅临时使用地加载到图形中时，TTL配置很有用。定义TTL消除了手动清理的需要，并非常有效地处理了删除操作。例如，对于TTL事件边缘（例如用户页面访问）是有意义的，例如在一段时间后或不再需要分析或操作查询处理时对用户页面访问进行汇总。一下的存储 backends 支持 edge 和 vertex TTL.

- Cassandra
- HBase
- BerkeleyDB - supports only hour-discrete TTL, thus the minimal TTL is one hour.

#### Edge TTL

Edge TTL是在每个Edge Label的基础上定义的，这意味着该标签的所有边缘具有相同的生存时间。请注意，后端必须支持单元级别的TTL。当前，只有Cassandra，HBase和BerkeleyDB支持此功能。

