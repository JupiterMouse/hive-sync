# coding = utf-8

from pyhive import hive
import re
import logging
import paramiko
import time
import datetime
import configparser


def get_schema_tables(path):
    file = open(path)
    lines = file.readlines()  # 读取全部内容 ，并以列表方式返回
    # 去换行符
    for i in range(len(lines)):
        lines[i] = lines[i].strip("\n")
    # 表信息
    schema = lines[0]
    tables = []
    # 去重，去\n
    for item in lines[1:]:
        if not item in tables:
            tables.append(item)
    return schema, tables


def show_create_table(conn, schema, table):
    is_partition = False
    cursor = conn.cursor()
    sql_format = 'show create table {schema}.{table}'
    cursor.execute(sql_format.format(schema=schema, table=table))
    table_info = cursor.fetchall()
    # 字段sql
    field_sql = ''
    for cloumn in table_info:
        # 第一个）为字段
        field_sql = field_sql + cloumn[0]
        if not cloumn[0][-2:].find(")") == -1:
            break
    # 分区sql
    partition_sql = ''
    status = 0
    for cloumn in table_info:
        str_c = str(cloumn[0])
        # 第一个）为字段
        if not str_c.find('PARTITIONED BY') == -1 and status == 0:
            status = 1
            is_partition = True
        if status == 1:
            partition_sql = partition_sql + str_c
            if not str_c.find(")") == -1:
                break
    # 存储格式
    store_sql = ''
    status = 0
    for i in range(len(table_info)):
        str_c = ''.join(table_info[i])
        # 第一个）为字段
        if not str_c.find('ROW FORMAT SERDE') == -1:
            status = 1
            next_c = ''.join(table_info[i + 1])
            if next_c.find("orc.OrcSerde") > -1:
                store_sql = ' stored as orc'

    # 分隔符sql DELIMITED
    delimited_sql = ''
    status = 0
    for i in range(len(table_info)):
        str_c = ''.join(table_info[i])
        # 第一个）为字段
        if not str_c.find('ROW FORMAT DELIMITED') == -1:
            status = 1
        if status:
            delimited_sql = delimited_sql + str_c
            next_c = ''.join(table_info[i + 1])
            if next_c.find("FIELDS") is -1 and next_c.find("LINES") is -1:
                break

    delimited_sql = delimited_sql + store_sql
    cursor.close()
    return field_sql, partition_sql, delimited_sql, is_partition


def build_partition_common_table(conn, field_sql, partitin_sql, delimited_sql, dt):
    m = re.match(".*\((.*)\).*", partitin_sql)
    # 分区字段头部
    i = len(field_sql) - 1
    field_sql = field_sql[0:i] + ',' + m.group(1) + field_sql[i:]

    # 截取第一个括号 eas_t_gl_assistbalance_5f`(
    i = field_sql.find("(") - 1
    field_sql = field_sql[0:i] + '_' + dt + field_sql[i:]
    cursor = conn.cursor()
    # 加在头部
    sql = field_sql + delimited_sql
    logging.info('分区表建全量表sql:%s', sql)
    cursor.execute(sql)


def do_partition(from_conn, to_conn, field_sql, partitin_sql, delimited_sql, dt):
    # from建分区表对应的全量表
    build_partition_common_table(from_conn, field_sql, partitin_sql, delimited_sql, dt)
    build_partition_common_table(to_conn, field_sql, partitin_sql, delimited_sql, dt)
    # 建分区表
    do_to_create_table(to_conn, field_sql, partitin_sql + delimited_sql)


def do_to_create_table(to_conn, field_sql, delimited_sql):
    cursor = to_conn.cursor()
    sql_create_table = field_sql + delimited_sql
    logging.info('分区表sql：%s', sql_create_table)
    cursor.execute(sql_create_table)
    cursor.close()


def test_table_exists(conn, schema, table):
    is_exists = True
    cursor = conn.cursor()
    sql_format = 'show create table {schema}.{table}'
    cursor.execute(sql_format.format(schema=schema, table=table))
    try:
        cursor.fetchall()
    except Exception as e:
        if str(e).find('Table not found') is not -1:
            is_exists = False
        pass
    finally:
        cursor.close()
    return is_exists


def do_data_scp(ssh, to_conn, schema, table):
    scp_format = 'sudo -u hdfs hadoop distcp -overwrite hdfs://172.16.0.3:8020/apps/hive/warehouse/{schema}.db/{table} hdfs://PRODCLUSTER/data1/apps/hive/warehouse/{schema}.db/{table}'
    logging.info("scp:\n" + scp_format.format(schema=schema, table=table))
    stdin, stdout, stderr = ssh.exec_command(scp_format.format(schema=schema, table=table))
    # stdin  标准格式的输入，是一个写权限的文件对象
    # stdout 标准格式的输出，是一个读权限的文件对象
    # stderr 标准格式的错误，是一个写权限的文件对象
    logging.info(stdout.read().decode())
    load_format = "sudo -u hdfs hive -e \"LOAD DATA INPATH 'hdfs://PRODCLUSTER/data1/apps/hive/warehouse/{schema}.db/{table}' into table {schema}.{table}\""
    logging.info('load\n' + load_format.format(schema=schema, table=table))
    ssh.exec_command(scp_format.format(schema=schema, table=table))
    stdin, stdout, stderr = ssh.exec_command(scp_format.format(schema=schema, table=table))
    logging.info(stdout.read().decode())
    logging.info(stdout.read().decode())


def partition_to_common(from_conn, schema, table, dt):
    cursor = from_conn.cursor()
    sql_format = 'insert overwrite table {schema}.{table}_{dt} select * from {schema}.{table}'
    sql = sql_format.format(schema=schema, table=table, dt=dt)
    logging.info("partition_to_common:%s", sql)
    cursor.execute(sql)
    # cursor.fetchall()
    cursor.close()


def common_to_partition(ssh, from_conn, to_conn, schema, table, dt):
    partition = get_partition_filed(to_conn, schema, table)
    sql_format = 'insert overwrite table {schema}.{table} partition({partition}) select * from {schema}.{table}_{dt};\"'
    # 动态分区set hive.exec.dynamic.partition.mode=nonstrict;
    shell = "sudo -u hdfs hive -e \"set hive.exec.dynamic.partition.mode=nonstrict;set hive.exec.dynamic.partition=true;" + sql_format.format(
        schema=schema, table=table, partition=partition, dt=dt)
    print(shell)
    stdin, stdout, stderr = ssh.exec_command(shell)
    print(stdout.read().decode())
    # cursor.execute('set hive.exec.dynamici.partition=true')
    # cursor.execute('set hive.exec.dynamic.partition.mode=nonstrict')
    # cursor.execute(sql_format.format(schema=schema, table=table, partition=partition, dt=dt))
    # cursor.fetchall()
    cursor = to_conn.cursor()
    cursor.execute("drop table {schema}.{table}_{dt}".format(schema=schema, table=table, dt=dt))
    cursor.close()
    cursor = from_conn.cursor()
    cursor.execute("drop table {schema}.{table}_{dt}".format(schema=schema, table=table, dt=dt))
    cursor.close()


def get_partition_filed(conn, schema, table):
    cursor = conn.cursor()
    sql_format = 'desc {schema}.{table}'
    cursor.execute(sql_format.format(schema=schema, table=table))
    res = cursor.fetchall()
    partition_fileds = []
    status = 0
    for result in res:
        if status:
            if str(result[0]).startswith("#") or str(result[0]).strip('') == '':
                continue
            partition_fileds.append(str(result[0]))
        if str(result[0]).find('# Partition Information') > -1:
            status = 1
    return str(partition_fileds).strip('[').strip("]").replace("'", "")


def do_logging():
    log_file = 'sys_%s.log' % datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    # level：设置日志输出的最低级别，即低于此级别的日志都不会输出
    # format：设置日志的字符串输出格式
    log_format = '%(asctime)s[%(levelname)s]: %(message)s'
    logging.basicConfig(filename=log_file, level=logging.INFO, format=log_format)


if __name__ == '__main__':
    conf = configparser.ConfigParser()
    conf.read("config.ini")  # 文件路径
    schema, tables = get_schema_tables(conf.get('file', 'path'))
    from_conn = hive.Connection(host=conf.get('hive.from', 'host'),
                                port=conf.get('hive.from', 'port'),
                                username=conf.get('hive.from', 'username'))

    to_conn = hive.Connection(host=conf.get('hive.to', 'host'),
                              port=conf.get('hive.to', 'port'),
                              username=conf.get('hive.to', 'username'))

    # 创建一个ssh的客户端，用来连接服务器
    ssh = paramiko.SSHClient()
    # 创建一个ssh的白名单
    know_host = paramiko.AutoAddPolicy()
    # 加载创建的白名单
    ssh.set_missing_host_key_policy(know_host)
    # 连接服务器
    ssh.connect(
        hostname=conf.get('ssh.to', 'hostname'),
        port=conf.get('ssh.to', 'port'),
        username=conf.get('ssh.to', 'username'),
        password=conf.get('ssh.to', 'password')
    )
    do_logging()
    for table in tables:
        logging.info('===============================start====================================')
        logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>%s.%s<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<', schema, table)
        try:
            if not test_table_exists(from_conn, schema, table):
                logging.error('from table {schema}.{table} not exists'.format(schema=schema, table=table))
            if test_table_exists(to_conn, schema, table):
                logging.error('to table {schema}.{table} had exists'.format(schema=schema, table=table))
        except Exception:
            pass
        field_sql, partition_sql, delimited_sql, is_partition = show_create_table(from_conn, schema, table)
        if is_partition:
            # 获取当前时间戳
            dt = str(round(time.time()))
            logging.info('datetime now %s', dt)
            # 分区建表远端操作
            do_partition(from_conn, to_conn, field_sql, partition_sql, delimited_sql, dt)
            # 源端表
            partition_to_common(from_conn, schema, table, dt)
            # 数据传输
            do_data_scp(ssh, to_conn, schema, table + "_" + dt)
            #  普通表到分区表
        else:
            do_to_create_table(to_conn, field_sql, delimited_sql)
            # 数据传输
            do_data_scp(ssh, to_conn, schema, table)
        logging.info('===============================end====================================')

    from_conn.close()
    to_conn.close()
    ssh.close()
