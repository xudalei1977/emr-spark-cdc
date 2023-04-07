[toc]

### EMR Hudi Example

#### 1. 程序

1. MSK2Hudi: 实现Mysql CDC功能，从MSK接收数据，写入数仓ODS层的Hudi表
2. DWS2DM: 模拟实时数仓场景，读取数仓ODS层的Hudi表的数据，进行聚合，写入数仓DM层的Hudi表

#### 2. 环境 

```markdown
*  EMR 6.9.0 (spark 3.3.0 hudi 0.12.1)
```

#### 3. 编译 & 运行

##### 3.1 编译

```properties
# 编译 mvn clean package -Dscope.type=provided 
```

##### 3.2 数据准备

我们只要准备Mysql的数据就可以，Mysql里的数据会被MSK Connector捕捉并写入MSK. 在Mysql里生成表和数据的脚本如下：

```properties 
-- create databases
create database if not exists dev default character set utf8mb4 collate utf8mb4_general_ci;
use dev;

-- create taxi_order table
create table if not exists taxi_order
(
    id                 int unsigned auto_increment primary key COMMENT '自增id',
    status             int           not null comment '订单状态',
    age                int           not null comment '客户年龄',
    phone              bigint        not null comment '电话号码',
    email              varchar(64)   not null comment '注册邮箱',
    ip                 varchar(32)   not null comment 'IP地址',
    cardDate           varchar(32)   not null comment '信用卡到期日',
    creditCardNumber   varchar(64)   not null comment '信用卡号',
    startAddress       varchar(64)   not null comment '起点地址',
    endAddress         varchar(64)   not null comment '终点地址',
    carNumber          varchar(32)   not null comment '车牌号',
    carType            varchar(64)   not null comment '车辆类型',
    userName           varchar(32)   not null comment '乘客姓名',
    userID             varchar(32)   not null comment '身份证',
    driverName         varchar(32)   not null comment '司机姓名',
    driverRegisterDate varchar(32)   not null comment '司机注册日期',
    score              decimal(4, 2) not null comment '得分',
    startLatitude      decimal(9, 7) not null comment '起点纬度',
    startLongitude     decimal(9, 7) not null comment '起点经度',
    endLatitude        decimal(9, 7) not null comment '终点纬度',
    endLongitude       decimal(9, 7) not null comment '终点经度',
    money              decimal(9, 2) not null comment '总金额',
    createTS           int           not null comment '创建时间戳',
    eventTS            int           not null comment '事件更新时间戳',
    UNIQUE INDEX id_index (id ASC),
    INDEX status_index (status),
    INDEX age_index (age),
    INDEX createTS_index (createTS),
    INDEX eventTS_index (eventTS)
) engine = InnoDB default charset = utf8mb4;

insert into taxi_order values
(1,5,57,13426827401,'yeguiying@yahoo.com','192.178.148.204','11/28','36217498828449','山西省莹县牧野陶街h座 796615','甘肃省深圳县大兴宁德路E座 962065','1-357-12962-9','a','白荣','440205195905270468','徐秀荣','20201027',99.2,62.72474,74.4654936,62.191423,10.5832138,40.9,1680696444,1680696444),

(2,2,98,15514993025,'juan82@qiangjie.cn','4.232.42.9','08/26','3534377410392139','贵州省淮安市东城天津街r座 892634','青海省沈阳市沙湾银川路w座 349746','1-241-98316-X','d','张雪梅','511124193604106529','韦秀荣','20210426',75.65,46.84907,41.1724693,51.528196,10.7994027,5.13,1680696444,1680696444),

(3,8,5,15878533774,'ipeng@81.cn','203.1.62.49','11/26','4443185819614034922','云南省玉梅市普陀陈路s座 345041','黑龙江省慧县怀柔翟路Z座 417394','1-63595-791-5','a','颜慧','620321194812189593','刘阳','20200920',80.32,22.3094215,93.4341685,31.6252125,41.5724324,72.45,1680696444,1680696444),

(4,5,58,15213873127,'xiulan02@pinglei.cn','198.51.178.129','04/24','349507865194637','重庆市成都市东城安路d座 581008','广东省晶县合川田路S座 545313','1-4896-4464-4','b','李冬梅','140830198011152173','罗欣','20190123',51.68,66.7981187,90.227271,89.7003504,54.6582081,64.59,1680696444,1680696444),

(5,5,19,18845077755,'guiyingwei@hotmail.com','192.84.185.155','02/26','4828392416236','陕西省文市江北谢路K座 265554','辽宁省佳县南湖李路m座 817510','0-01-501983-7','a','张玉英','411502196701214875','刘秀荣','20190609',30.55,77.674622,39.329482,49.8479129,15.3892915,14.0,1680696444,1680696444),

(6,9,83,13331341573,'xiulanwei@yahoo.com','192.96.206.242','02/26','4878673183161','北京市合山县璧山谢路J座 446761','青海省石家庄市西峰辛集街p座 606506','0-948387-69-6','c','张帆','410303196708316086','张玉珍','20210530',49.75,97.4059628,40.7465021,14.5719341,71.1143136,8.91,1680696444,1680696444),

(7,4,19,15113337614,'pingfeng@yang.cn','192.88.97.62','05/30','5571247891213303','重庆市北镇县新城荆门路X座 374456','重庆市北京县新城兰州街o座 773835','1-270-00607-X','c','汪欢','61082819820916981X','袁玉华','20170716',74.56,24.2539387,76.4859106,71.9002189,57.5651823,70.49,1680696444,1680696444),

(8,3,7,13674028418,'yan50@gmail.com','192.0.3.53','04/27','30584480252359','内蒙古自治区雷市华龙谌街a座 105305','江苏省金凤县南湖阜新街M座 551597','1-85515-247-9','d','尹峰','500235195709196744','鞠晶','20200625',83.0,71.8851512,81.5763537,75.27066,73.342259,79.79,1680696444,1680696444),

(9,6,72,13478278596,'gang97@gmail.com','198.51.96.54','11/28','4220104288834043','甘肃省重庆市友好合肥路o座 440179','香港特别行政区玉梅市秀英张街r座 388690','1-56012-004-5','d','吴玉兰','420529198309018669','吴凤英','20200616',22.97,93.9622384,70.121134,70.6894082,16.352541,89.75,1680696444,1680696444),

(10,5,74,15228642270,'ming26@hotmail.com','192.42.214.19','07/31','340935760171617','江西省佛山县涪城石家庄路K座 160066','江苏省玉兰市永川海门路H座 452837','1-80608-251-9','b','尹莉','140981194104235733','张莹','20180915',39.7,49.219574,84.8503627,29.2373297,95.1466674,12.89,1680696444,1680696444);

```


##### 3.3 运行程序说明

* 支持参数

```properties
  MSK2Hudi 1.0
  Usage: Spark MSK2Hudi [options]
  
    -e, --env <value>               env: dev or prod
    -b, --brokerList <value>        kafka broker list,sep comma
    -t, --sourceTopic <value>       kafka topic
    -p, --consumeGroup <value>      kafka consumer group
    -s, --syncHive <value>          whether sync hive，default:false
    -o, --startPos <value>          kafka start pos latest or earliest,default latest
    -i, --trigger <value>           default 300 second,streaming trigger interval
    -c, --checkpointDir <value>     hdfs dir which used to save checkpoint
    -g, --hudiEventBasePath <value> hudi event table hdfs base path
    -s, --syncDB <value>            hudi sync hive db
    -u, --syncTableName <value>     hudi sync hive table name
    -y, --tableType <value>         hudi table type MOR or COW. default COW
    -r, --syncJDBCUrl <value>       hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000
    -n, --syncJDBCUsername <value>  hive server2 jdbc username, default: hive
    -p, --partitionNum <value>      repartition num,default 16
    -t, --morCompact <value>        mor inline compact,default:true
    -m, --inlineMax <value>         inline max compact,default:20
    -w, --hudiWriteOperation <value>    hudi write operation,default insert
    -z, --hudiKeyField <value>      hudi key field, recordkey
    -k, --hudiCombieField <value>   hudi key field, precombine
    -q, --hudiPartition <value>     hudi partition column,default logday,hm
```

* 启动样例

```shell
  # 1. --packages 添加的parkcage依赖 
  # 2. --jars 添加的jar包依赖，注意替换S3的桶为自己的桶
  # 3. --class 运行的主程序
  # 4. -b 参数注意替换为你自己的kafka地址
  # 5. -t 替换为你自己的kafka topic
  # 6. -i 表示streming的trigger interval，10表示10秒
  # 7. -y 表示hudi表的类型 cow，还是 mor
  # 7. -g 表示存储hudi表的根目录
  # 7. -s 表示hudi表的数据库
  # 7. -u 表示hudi表的名称
  # 7. -w 参数配置 upsert，还是insert
  # 8. -z 分别表示hudi的recordkey 字段是什么
  # 8. -k 分别表示hudi的precombine 字段是什么
  # 9. -q 表示hudi分区字段
  # 10. 其他参数按照支持的参数说明修改替换
  
  spark-submit --master yarn --deploy-mode client --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 4 \
  --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.12.1,org.apache.spark:spark-avro_2.12:3.3.0,org.apache.hudi:hudi-client:0.12.1,org.apache.hudi:hudi-hadoop-mr-bundle:0.12.1 \
  --jars /usr/lib/spark/external/lib/spark-sql-kafka-0-10.jar,/usr/lib/spark/external/lib/spark-streaming-kafka-0-10-assembly.jar,/usr/lib/hudi/cli/lib/kafka-clients-2.4.1.jar,s3://airflow-us-east-1-551831295244/jar/commons-pool2-2.11.1.jar,s3://airflow-us-east-1-551831295244/jar/aws-msk-iam-auth-1.1.1-all.jar,s3://airflow-us-east-1-551831295244/jar/scopt_2.12-4.0.0-RC2.jar \
  --class com.aws.analytics.MSK2Hudi s3://airflow-us-east-1-551831295244/jar/emr-spark-cdc-1.0-SNAPSHOT.jar \
  -e prod \
  -b b-1.emrworkshopmsk.v4iilt.c14.kafka.us-east-1.amazonaws.com:9098,b-2.emrworkshopmsk.v4iilt.c14.kafka.us-east-1.amazonaws.com:9098,b-3.emrworkshopmsk.v4iilt.c14.kafka.us-east-1.amazonaws.com:9098 \
  -t mysql.dev.taxi_order -p msk-consumer-group-01 -o latest -c /user/hadoop/checkpoint/ -i 60 \
  -y cow -g s3://airflow-us-east-1-551831295244 -s dev -u taxi_order \
  -w upsert -z id -k createTS -q age
```

