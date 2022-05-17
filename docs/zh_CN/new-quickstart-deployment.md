在我们拿到源代码之后，我们需要依次编译并启动DN节点和CN节点，同时需要注意的是，在启动时，我们需要使用非root账号。
> 注：以下的示例为一台刚购买的ECS，系统为ubuntu 20。

# 创建普通用户并添加相应权限
```sql
#创建用户并初始化密码
sudo adduser polarx

#添加root权限
sudo vim /etc/sudoers

#添加
user    ALL=(ALL:ALL) ALL
```
# 编译并启动存储节点（DN）
## 编译DN节点
### 安装依赖
```sql
apt-get update

# 安装GCC7
apt install -y gcc-7 g++-7
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 \
                         --slave /usr/bin/g++ g++ /usr/bin/g++-7 
update-alternatives --config gcc
gcc --version
g++ --version

# 安装依赖
apt install make automake cmake git bison libaio-dev libncurses-dev libsasl2-dev libldap2-dev libssl-dev pkg-config
```
### 上传galaxyengine代码并解压
### 编译
```sql
# 进入 galaxyengine 代码路径
cd galaxyengine

# 安装boost1.70 (注：把boost放到仓库里避免下载）
wget https://boostorg.jfrog.io/artifactory/main/release/1.70.0/source/boost_1_70_0.tar.gz
mkdir extra/boost
cp boost_1_70_0.tar.gz extra/boost/

# 编译安装
# 详细参数请参考 https://dev.mysql.com/doc/refman/8.0/en/source-configuration-options.html
cmake .                                   \
    -DFORCE_INSOURCE_BUILD=ON           \
    -DCMAKE_BUILD_TYPE="Debug"          \
    -DSYSCONFDIR="/u01/mysql"           \
    -DCMAKE_INSTALL_PREFIX="/u01/mysql" \
    -DMYSQL_DATADIR="/u01/mysql/data"   \
    -DMYSQL_MAINTAINER_MODE=0           \
    -DWITH_SSL=openssl                  \
    -DWITH_BOOST="./extra/boost/boost_1_70_0.tar.gz" \
    -DWITH_XENGINE_STORAGE_ENGINE=OFF
               
make -j8
make install
```
###  创建mysql配置文件my.cnf
```sql
[mysqld]
socket = /u01/my3306/run/mysql.sock
datadir = /u01/my3306/data
tmpdir = /u01/my3306/tmp
log-bin = /u01/my3306/mysql/mysql-bin.log
log-bin-index = /u01/my3306/mysql/mysql-bin.index
# log-error = /u01/my3306/mysql/master-error.log
relay-log = /u01/my3306/mysql/slave-relay.log
relay-log-info-file = /u01/my3306/mysql/slave-relay-log.info
relay-log-index = /u01/my3306/mysql/slave-relay-log.index
master-info-file = /u01/my3306/mysql/master.info
slow_query_log_file = /u01/my3306/mysql/slow_query.log
innodb_data_home_dir = /u01/my3306/mysql
innodb_log_group_home_dir = /u01/my3306/mysql

port = 4886
loose_polarx_port = 32886
loose_galaxyx_port = 32886
loose_polarx_max_connections = 5000

loose_server_id = 476984231
loose_cluster-info = 127.0.0.1:14886@1
loose_cluster-id = 5431
loose_enable_gts = 1
loose_innodb_undo_retention=1800



core-file
loose_log_sql_info=1
loose_log_sql_info_index=1
loose_indexstat=1
loose_tablestat=1
default_authentication_plugin=mysql_native_password

# close 5.6 variables for 5.5
binlog_checksum=CRC32
log_bin_use_v1_row_events=on
explicit_defaults_for_timestamp=OFF
binlog_row_image=FULL
binlog_rows_query_log_events=ON
binlog_stmt_cache_size=32768

#innodb
innodb_data_file_path=ibdata1:100M;ibdata2:200M:autoextend
innodb_buffer_pool_instances=8
innodb_log_files_in_group=4
innodb_log_file_size=200M
innodb_log_buffer_size=200M
innodb_flush_log_at_trx_commit=1
#innodb_additional_mem_pool_size=20M #deprecated in 5.6
innodb_max_dirty_pages_pct=60
innodb_io_capacity_max=10000
innodb_io_capacity=6000
innodb_thread_concurrency=64
innodb_read_io_threads=8
innodb_write_io_threads=8
innodb_open_files=615350
innodb_file_per_table=1
innodb_flush_method=O_DIRECT
innodb_change_buffering=none
innodb_adaptive_flushing=1
#innodb_adaptive_flushing_method=keep_average #percona
#innodb_adaptive_hash_index_partitions=1      #percona
#innodb_fast_checksum=1                       #percona
#innodb_lazy_drop_table=0                     #percona
innodb_old_blocks_time=1000
innodb_stats_on_metadata=0
innodb_use_native_aio=1
innodb_lock_wait_timeout=50
innodb_rollback_on_timeout=0
innodb_purge_threads=1
innodb_strict_mode=1
#transaction-isolation=READ-COMMITTED
innodb_disable_sort_file_cache=ON
innodb_lru_scan_depth=2048
innodb_flush_neighbors=0
innodb_sync_array_size=16
innodb_print_all_deadlocks
innodb_checksum_algorithm=CRC32
innodb_max_dirty_pages_pct_lwm=10
innodb_buffer_pool_size=500M

#myisam
concurrent_insert=2
delayed_insert_timeout=300

#replication
slave_type_conversions="ALL_NON_LOSSY"
slave_net_timeout=4
skip-slave-start=OFF
sync_master_info=10000
sync_relay_log_info=1
master_info_repository=TABLE
relay_log_info_repository=TABLE
relay_log_recovery=0
slave_exec_mode=STRICT
#slave_parallel_type=DATABASE
slave_parallel_type=LOGICAL_CLOCK
loose_slave_pr_mode=TABLE
slave-parallel-workers=32

#binlog
server_id=193317851
binlog_cache_size=32K
max_binlog_cache_size=2147483648
loose_consensus_large_trx=ON
max_binlog_size=500M
max_relay_log_size=500M
relay_log_purge=OFF
binlog-format=ROW
sync_binlog=1
sync_relay_log=1
log-slave-updates=0
expire_logs_days=0
rpl_stop_slave_timeout=300
slave_checkpoint_group=1024
slave_checkpoint_period=300
slave_pending_jobs_size_max=1073741824
slave_rows_search_algorithms='TABLE_SCAN,INDEX_SCAN'
slave_sql_verify_checksum=OFF
master_verify_checksum=OFF

# parallel replay
binlog_transaction_dependency_tracking = WRITESET
transaction_write_set_extraction = XXHASH64


#gtid
gtid_mode=OFF
enforce_gtid_consistency=OFF

loose_consensus-io-thread_cnt=8
loose_consensus-worker-thread_cnt=8
loose_consensus_max_delay_index=10000
loose_consensus-election-timeout=10000
loose_consensus_max_packet_size=131072
loose_consensus_max_log_size=20M
loose_consensus_auto_leader_transfer=ON
loose_consensus_log_cache_size=536870912
loose_consensus_prefetch_cache_size=268435456
loose_consensus_prefetch_window_size=100
loose_consensus_auto_reset_match_index=ON
loose_cluster-mts-recover-use-index=ON
loose_async_commit_thread_count=128
loose_replicate-same-server-id=on
loose_commit_lock_done_count=1
loose_binlog_order_commits=OFF
loose_cluster-log-type-node=OFF

#thread pool
# thread_pool_size=32
# thread_pool_stall_limit=30
# thread_pool_oversubscribe=10
# thread_handling=pool-of-threads

#server
default-storage-engine=INNODB
character-set-server=utf8
lower_case_table_names=1
skip-external-locking
open_files_limit=615350
safe-user-create
local-infile=1
sql_mode='NO_ENGINE_SUBSTITUTION'
performance_schema=0


log_slow_admin_statements=1
loose_log_slow_verbosity=full
long_query_time=1
slow_query_log=0
general_log=0
loose_rds_check_core_file_enabled=ON

table_definition_cache=32768
eq_range_index_dive_limit=200
table_open_cache_instances=16
table_open_cache=32768

thread_stack=1024k
binlog_cache_size=32K
net_buffer_length=16384
thread_cache_size=256
read_rnd_buffer_size=128K
sort_buffer_size=256K
join_buffer_size=128K
read_buffer_size=128K

# skip-name-resolve
#skip-ssl
max_connections=36000
max_user_connections=35000
max_connect_errors=65536
max_allowed_packet=1073741824
connect_timeout=8
net_read_timeout=30
net_write_timeout=60
back_log=1024

loose_boost_pk_access=1
log_queries_not_using_indexes=0
log_timestamps=SYSTEM
innodb_read_ahead_threshold=0

loose_io_state=1
loose_use_myfs=0
loose_daemon_memcached_values_delimiter=':;:'
loose_daemon_memcached_option="-t 32 -c 8000 -p15506"

innodb_doublewrite=1
```
## 启动PolarDB-X DN

- 此步骤启动一个mysql进程，作为metadb和dn
- 参考附录中的mysql配置文件（my.cnf），可进行相应修改，默认使用 4886 作为 mysql端口，32886 作为私有协议端口
- 默认使用 /u01/my3306 作为mysql数据目录，可以修改成其他目录

注意：启动 DN 需要使用非 root 账号完成，如使用上述创建的polarx
### 启动mysql：
```sql
#需要切换到普通用户
su polarx

# 若无/u01/my3306的权限，则赋予相关权限，chmod 777 /u01/my3306
mkdir -p /u01/my3306/{data,log,run,tmp,mysql}
/u01/mysql/bin/mysqld --defaults-file=my.cnf --initialize-insecure
/u01/mysql/bin/mysqld --defaults-file=my.cnf
```
### 验证是否启动成功
```sql
mysql -h127.0.0.1 -uroot -P4886
show databases;
```
# 编译并启动CN
## 编译 PolarDB-X CN (计算节点，代号GalaxySQL)
### 上传galaxysql源代码并解压
### 编译CN节点
```sql
# 安装依赖 JDK 1.8 和 Maven 3
apt-get install openjdk-8-jdk
apt install maven

# 进入代码目录 
cd galaxysql/

# 上传galaxyglue源代码，解压后放入galaxysql/polardbx-rpc目录下

# 编译打包
mvn install -D maven.test.skip=true -D env=release 

# 解压运行
tar zxvf target/polardbx-server-5.4.13-SNAPSHOT.tar.gz

#后续操作使用普通账户即可，且不能使用root账户启动
su poalrx
```
## 在DN中创建内部账户
```sql
#登录到DN中
mysql -h127.0.0.1 -uroot -P4886

use mysql;

create user internal_user identified by 'Polarx123456';
create user 'internal_user'@'localhost' identified by 'Polarx123456';
create user 'internal_user'@'127.0.0.1' identified by 'Polarx123456';

grant all privileges on *.* to 'internal_user'@'%' ;
grant all privileges on *.* to 'internal_user'@'localhost';
grant all privileges on *.* to 'internal_user'@'127.0.0.1' ;

update user set Grant_priv = 'Y' where user = 'internal_user';

flush privileges;
```
## 启动PolarDB-X CN
启动mysql进程之后，便可以初始化PolarDB-X，需要准备以下几个配置：

- metadb user：以下采用my_polarx
- metadb database：创建metadb库，以下采用 polardbx_meta_db_polardbx
- 密码加密key（dnPasswordKey)：以下采用 asdf1234ghjk5678
- PolarDB-X默认用户名：默认为 polarx_root
- PolarDB-X默认用户密码：默认为 123456，可通过 -S 参数修改
> 注意：启动 CN 需要使用非 root 账号完成

### 修改配置文件 
修改conf/server.properties，逐个替换以下配置项：
```sql
# PolarDB-X 服务端口
serverPort=8527
# PolarDB-X RPC 端口
rpcPort=9090
 # MetaDB地址
metaDbAddr=127.0.0.1:4886
# MetaDB私有协议端口
metaDbXprotoPort=32886
# MetaDB用户
metaDbUser=my_polarx
metaDbName=polardbx_meta_db_polardbx
# PolarDB-X实例名
instanceId=polardbx-polardbx
```
在上述文件中添加如下内容，即上节中创建的内部账户
```sql
rootUser=internal_user
rootPasswd=Polarx123456
```
### 初始化PolarDB-X：

- -I: 进入初始化模式
- -P: 之前准备的dnPasswordKey
- -d: DataNode的地址列表，单机模式下就是之前启动的mysql进程的端口和地址
- -r: 连接metadb的密码
- -u: 为PolarDB-X创建的根用户
- -S: 为PolarDB-X创建的根用户密码
```sql
bin/startup.sh \
    -I \
    -P asdf1234ghjk5678 \
    -d 127.0.0.1:4886:32886 \
    -r "" \
    -u polardbx_root \
    -S "123456"
```
此步骤中会生成内部密码及加密密码，需要将其填写配置文件 conf/server.properties 中，用于后续访问:
```sql
======== Paste following configurations to conf/server.properties ! ======= 
metaDbPasswd=HMqvkvXZtT7XedA6t2IWY8+D7fJWIJir/mIY1Nf1b58=
```
若上述初始化失败，重新初始化时需要清理DN状态（执行下面代码），若成功，则无需如下操作
```sql
#登录到DN中
mysql -h127.0.0.1 -uroot -P4886

drop database polardbx_meta_db_polardbx;
drop user 'my_polarx'@'%';
drop user 'my_polarx'@'localhost';
drop user 'my_polarx'@'127.0.0.1';

# 同时需要删除conf/server.properties中的metaDbPasswd，重新生成后填入新的
```
### 启动PolarDB-X：
```sql
bin/startup.sh -P asdf1234ghjk5678
```
### 登录数据库并进行验证
```sql
mysql -h127.1 -P8527 -upolardbx_root
```
