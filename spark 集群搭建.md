# spark 集群搭建

## 软件环境
1. centos: 7.4.1708 (Core)
2. hadoop: 2.6.5
3. spark: 1.6.0

## 环境准备
### 修改主机名

在每台主机上修改hosts 文件
```
10.1.61.168   master
10.1.61.141   slave1
10.1.61.144   slave2
```

同时对各个主机命名特有的主机名， 不要都用localhost， 比如：
```
> hostnamectl hostname host-10.1.61.141
```
同时也将主机名写入 /etc/hosts 文件
```
10.1.61.141  host-10.1.61.141
```

对三台服务器都做以上操作


### 配置ssh 互通
在各自机器上生成公钥和私钥对
```
    ssh-keygen -t rsa   #一路回车
```
在各个机器上，将所有公钥加到用于认证的公钥文件authorized_keys中
```
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    
    scp ~/.ssh/authorized_keys root@slave1:~/.ssh/

```


## 安装java
### 直接yum 安装 openjdk 1.8
```
> yum install java-1.8.0-openjdk  java-1.8.0-openjdk-devel
```

### 配置java环境变量
编辑/etc/profile 配置文件
```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export JRE_HOME=$JAVA_HOME/jre
export PATH=$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$PATH
export CLASSPATH=$CLASSPATH:.:$JAVA_HOME/lib:$JAVA_HOME/jre/lib
```
source /etc/profile 使环境变量生效

### 安装scala
安装目录： /opt/scala
```
    tar -zxvf scala-2.10.4.tgz
```
配置scala环境变量, 修改/etc/profile 配置文件
```
    export PATH=$PATH:/opt/scala/scala-2.10.4/bin
```
source /etc/profile 使环境变量生效


## 安装配置hadoop yarn
###  下载配置hadoop-2.6.5
新建目录　/opt/hadoop
解压
```
mkdir /opt/hadoop/
tar -zxvf hadoop-2.6.5.tar.gz

```

###配置hadoop
在 /opt/hadoop/hadoop-2.6.5/etc/hadoop 进入hadoop配置目录, 需要配置有一下7个文件：
hadoop-env.sh, yarn-env.sh, slaves, core-site.xml, maprd-site.xml, yarn-site.xml,
把配置好的文件分发给所有slaves

1. 在 hadoop-env.sh 中配置JAVA_HOME
```
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0
```
如果ssh端口不是默认的22，在etc/hadoop/hadoop-env.sh里改下。如：
```
export HADOOP_SSH_OPTS="-p 9166"
```

2. 在yarn-env.sh 中配置JAVA_HOME

```
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0
```

3. 在slaves中配置slave节点的ip或者host
```
    slave1
    slave2
```

4. 修改core-site.xml
```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000/</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>file:/data/hadoop/tmp</value>
    </property>
</configuration>

```

5. 修改hdfs-site.xml
```
<configuration>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>master:9001</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/data/hadoop/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/data/hadoop/dfs/data</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
</configuration>
```

6. 修改mapred-site.xml
```
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>

```

7. 修改yarn-site.xml
```
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>master:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>master:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>master:8035</value>
    </property>
    <property>
        <name>yarn.resourcemanager.admin.address</name>
        <value>master:8033</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>master:8090</value>
    </property>
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>

    <property>
        <name>yarn.log.server.url</name>
        <value>http://master:19888/jobhistory/logs/</value>
    </property>

    <property>
        <description>Where to aggregate logs to.</description>
        <name>yarn.nodemanager.remote-app-log-dir</name>
        <value>/tmp/logs</value>
    </property>

    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>259200</value>
    </property>

    <property>
        <name>yarn.log-aggregation.retain-check-interval-seconds</name>
        <value>3600</value>
    </property>

    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>102400</value>
    </property>

    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>102400</value>
    </property>
    
    
</configuration>
    
```

### 启动 Hadoop
在master上执行一下操作， 就可以启动hadoop了

```
bin/hadoop namenode -format     #格式化namenode
sbin/start-dfs.sh               #启动dfs 
sbin/start-yarn.sh              #启动yarn
```

###验证 Hadoop 是否安装成功
可以通过jps命令查看各个节点启动的进程是否正常。在master上应该有以下几个进程:

```
$ jps  #run on master
3407 SecondaryNameNode
3218 NameNode
3552 ResourceManager
3910 Jps
```

在每个slave上应该有一下几个进程
```
$ jps   #run on slaves
2072 NodeManager
2213 Jps
1962 DataNode
```

或者在浏览器中输入 http://master:8088 ，应该有 hadoop 的管理界面出来了，并能看到 slave1 和 slave2 节点。

## spark 安装
### 下载解压
新建 /opt/spark
```
tar -zxvf spark-1.6.0-bin-hadoop2.6.tgz
```

### 配置Spark

```
cd /opt/spark/spark-1.6.0-bin-hadoop2.6/conf  #进入spark配置目录
cp spark-env.sh.template spark-env.sh   #从配置模板复制
vi spark-env.sh     #添加配置内容
在spark-env.sh末尾添加以下内容（这是我的配置，你可以自行修改）：

export JAVA_HOME=/usr/lib/jvm/java-1.8.0
export HADOOP_HOME=/opt/hadoop/hadoop-2.6.5
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
SPARK_MASTER_IP=master
SPARK_LOCAL_DIRS=/opt/spark/spark-1.6.0-bin-hadoop2.6
SPARK_DRIVER_MEMORY=1G

```
如果ssh端口不是默认的22，在spark-env.sh里改下。如：
```
export SPARK_SSH_OPTS="-p 9166"
```


### 配置slaves
```
cp slaves.template slaves

vi slaves在slaves文件下填上slaves主机名:
slave1
slave2

```
将配置好的文件 spark_env.sh slaves 分发给所有slaves


### 启动Spark
sbin/start-all.sh


### 验证spark是否安装成功
用jps检查， 在master上用该有以下几个进程:
```
$ jps
7949 Jps
7328 SecondaryNameNode
7805 Master
7137 NameNode
7475 ResourceManager
```

在slave上应该有以下几个进程:
```
$jps
3132 DataNode
3759 Worker
3858 Jps
3231 NodeManager

```


### 运行实例
```
#本地模式两线程运行
./bin/run-example SparkPi 10 --master local[2]
#Spark Standalone 集群模式运行
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://master:7077 \
  lib/spark-examples*.jar \
  100
#Spark on YARN 集群上 yarn-cluster 模式运行
./bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master yarn-cluster \  # can also be `yarn-client`
    lib/spark-examples*.jar \
    10
```

注意 Spark on YARN 支持两种运行模式，分别为yarn-cluster和yarn-client，具体的区别可以看这篇博文，从广义上讲，yarn-cluster适用于生产环境；而yarn-client适用于交互和调试，也就是希望快速地看到application的输出。

### 查看正在运行的应用:
```
查看
/opt/hadoop/hadoop-2.6.5/bin/yarn application -list

删除应用
/opt/hadoop/hadoop-2.6.5/bin/yarn application -kill application_1524663570549_0023
```

### spark 消费kafka命令:
```
bin/spark-submit --master yarn-client  --jars lib/spark-examples-1.6.0-hadoop2.6.0.jar,lib/spark-streaming-kafka-assembly_2.10-1.6.1.jar  examples/src/main/python/streaming/kafka_wordcount.py 10.1.61.144:2181 test
```

单机环境
```
./bin/spark-submit --driver-memory 32G --executor-memory 32G --jars ./lib/spark-streaming-kafka-assembly_2.10-1.6.1.jar /opt/work/performance_analysis/total_data_statistics.py 10.11.0.94:2181/kafka performance
```

集群环境
```
bin/spark-submit --driver-memory 16G --num-executors 2 --executor-memory 16G --executor-cores 32 --master yarn-cluster  --jars lib/spark-examples-1.6.0-hadoop2.6.0.jar,lib/spark-streaming-kafka-assembly_2.10-1.6.1.jar --files /opt/work/performance_analysis/svrip_list.txt  /opt/work/performance_analysis/total_data_statistics.py 10.11.0.94:2181/kafka performance
```


### 性能优化
rdd  使用cache

partition分区足够大， 一般来说是 num-executors * executor-cores 的2到3倍