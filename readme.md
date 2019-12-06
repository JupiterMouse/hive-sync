## 集群间Hive迁移数据

- [x] 支持hive集群间数据迁移
- [x] 支持普通表与分区表
- [x] 支持ORC格式压缩表



### 迁移原理

#### distcp

* Hadoop自带的集群间copy工具[distcp](http://hadoop.apache.org/docs/r1.2.1/distcp.html)

​		distcp（分布式拷贝）是用于大规模集群内部和集群之间拷贝的工具。 它使用Map/Reduce实现文件分发，错误处理和恢复，以及报告生成。 它把文件和目录的列表作为map任务的输入，每个任务会完成源列表中部分文件的拷贝。 由于使用了Map/Reduce方法，这个工具在语义和执行上都会有特殊的地方。

* example

  -update 为更新模式

  -overwrite 覆盖模式

  ```shell
  hadoop distcp  -update hdfs://x.x.x.x:8020/apps/hive/warehouse/${scheme}.db/${db} hdfs://PRODCLUSTER/data1/apps/hive/warehouse/${scheme}.db/${db}
  ```


#### 分区表支持

* 建分区表对应的非分区表

  通过show create table x x x  获取字段信息,分区字段信息，在源端和目标端建立同样的非分区表。

* 源端分区表导入非分区表

  ```shell
  insert overwrite table {schema}.{table}_{dt} select * from {schema}.{table}
  ```

* 同步非分区表到目标端

  ```shell
  sudo -u hdfs hadoop distcp -overwrite hdfs://xxx/{schema}.db/{table} hdfs://xxx/{schema}.db/{table}
  ```

* 目标端导入数据到hive

  ```shell
  LOAD DATA INPATH 'hdfs://PRODCLUSTER/data1/apps/hive/warehouse/{schema}.db/{table}' into table {schema}.{table}
  ```

* 目标端非分区表到分区表

  ```shell
  insert overwrite table {schema}.{table}_{dt} select * from {schema}.{table}
  ```

### 脚本使用

* 下载依赖包

  pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

* 准备同步表信息

  example

  ```shell
  xx_dw  # 第一行为schema,第二行之后为table
  dw_md_store_info
  dw_md_shopguide_info
  dw_md_shopguide_changelog
  dw_md_s4_store
  dw_md_retail_store
  ```

* config.ini配置

  example

  ```shell
  [hive.from]
  host=xx.xx.xx.xx # 源hive ip
  port=10000
  username=hive
  
  [hive.to]
  host=xx.xx.xx.xx # 目标hive ip
  port=10000
  username=hive
  
  [ssh.to]
  hostname=xx.xx.xx.xx # 目标ssh
  port=22
  username=xxxx
  password=xxxx
  
  [file]
  path=sync/xx.txt # 同步schema.table信息
  ```

