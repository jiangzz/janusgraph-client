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

## 安装[graphexp](https://github.com/bricaud/graphexp)

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

![img](./assets/image-20210324104947478.png)

## Java API集成

①导入maven依赖

```XML
<!-- https://mvnrepository.com/artifact/org.janusgraph/janusgraph-core -->
<dependency>
    <groupId>org.janusgraph</groupId>
    <artifactId>janusgraph-core</artifactId>
    <version>0.5.3</version>
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
</dependency>
```