import datetime
import tushare as ts
import pymysql
from pymysql.converters import escape_str
import pandas as pd
import numpy as np
import timeit
import sys
from time import sleep
#import autokeras as ak
from _ast import Try
import logging

def make_table_sql(table_name, df, uk_name, uk):
    columns = df.columns.tolist()
    types = df.ftypes
    # 添加id 制动递增主键模式
    make_table = []
    for item in columns:
        if 'int' in types[item]:
            char = '`' + item + '` INT'
        elif 'float' in types[item]:
            char = '`' + item + '` FLOAT'
        elif 'object' in types[item]:
            char = '`' + item + '` VARCHAR(255)'           
        elif 'datetime' in types[item]:
            char = '`' + item + '` DATETIME'            
        make_table.append(char)
    fields = ','.join(make_table)
    create_table_sql = 'CREATE TABLE IF NOT EXISTS `{}` ({}, UNIQUE KEY `{}` ({}))'.format(table_name, fields, uk_name, uk)
    logging.debug(create_table_sql)
    return create_table_sql

def make_replace_sql(table_name, df):
    columns = df.columns.tolist()
    # 添加id 制动递增主键模式
    make_table = []
    for item in columns:
        char = '`' + item + '`'       
        make_table.append(char)
    fields =  ','.join(make_table)
    # 根据columns个数
    value_format = ','.join(['%s' for _ in range(len(df.columns))])
    # executemany批量操作 插入数据 批量操作比逐个操作速度快很多
    replace_sql = 'REPLACE INTO {} ({}) VALUES ({})'.format(table_name, fields, value_format)
    logging.debug(replace_sql)
    return replace_sql


def df2mysql(conn, cursor, db_name, table_name, df, uk, drop_table=False):
    # 创建database
    cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(db_name))
    # 选择连接database
    conn.select_db(db_name)
    # 创建table
    if drop_table:
        cursor.execute('DROP TABLE IF EXISTS `{}`'.format(table_name))
    uk_name = 'uk_' + uk.replace(',', '')
    create_table_sql = make_table_sql(table_name, df, uk_name, uk)
    cursor.execute(create_table_sql)
    values = df.values.tolist()
    for row_values in values:
        for i in range(len(row_values)):
            if str(row_values[i]) == 'nan':
                types = df.ftypes
                if 'int' in types[i]:
                    row_values[i] = 0
                elif 'float' in types[i]:
                    row_values[i] = 0.00
                elif 'object' in types[i]:
                    row_values[i] = null           
                elif 'datetime' in types[i]:
                    row_values[i] = null
                
    # executemany批量操作 插入数据 批量操作比逐个操作速度快很多
    rowcount = cursor.executemany(make_replace_sql(table_name, df), values)
    logging.debug('[{}] REPLACE {} rows into {}'.format(loopCount, rowcount, table_name))

def download_stock_data():
    stock_basic = pro.stock_basic(exchange='', list_status='', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    pd.DataFrame(stock_basic).to_csv(data_dir+'股票列表.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='stock_basic', df=stock_basic, uk='ts_code')
     
    trade_cal = pro.trade_cal()
    pd.DataFrame(trade_cal).to_csv(data_dir+'交易日历.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='trade_cal', df=trade_cal, uk='exchange,cal_date')
 
    namechange = pro.namechange()
    pd.DataFrame(namechange).to_csv(data_dir+'股票曾用名.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='namechange', df=namechange, uk='ts_code,name')
     
    hs_const_SH = pro.hs_const(hs_type='SH')
    hs_const_SZ = pro.hs_const(hs_type='SZ')
    hs_const = pd.concat([hs_const_SH,hs_const_SZ],ignore_index=True)
    pd.DataFrame(hs_const).to_csv(data_dir+'沪深股通成份股.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='hs_const', df=hs_const, uk='ts_code')
     
    stock_company = pro.stock_company()
    pd.DataFrame(stock_company).to_csv(data_dir+'上市公司基本信息.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='stock_company', df=stock_company, uk='ts_code')
 
    stk_managers = pro.stk_managers(ts_code=','.join(stock_pool))
    pd.DataFrame(stk_managers).to_csv(data_dir+'上市公司管理层.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='stk_managers', df=stk_managers, uk='ts_code,name')
     
    stk_rewards = pro.stk_rewards(ts_code=','.join(stock_pool))
    pd.DataFrame(stk_rewards).to_csv(data_dir+'管理层薪酬和持股.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='stk_rewards', df=stk_rewards, uk='ts_code,name')
     
    new_share = pro.new_share()
    pd.DataFrame(new_share).to_csv(data_dir+'IPO新股列表.csv', index=False)
    df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='new_share', df=new_share, uk='ts_code')

    daily = pd.DataFrame()
    for ts_code in list(stock_pool):
        logging.info("[{}] daily:{}".format(loopCount, ts_code))
        daily1 = pro.daily(ts_code=ts_code)
        df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='daily', df=daily1, uk='ts_code,trade_date')
    
    
def generate_train_data():
    train_data_header = True
    for ts_code in stock_pool:
        common_row = []
        cursor.execute("select ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs from `stock_basic` where ts_code='{}' ".format(ts_code))
        # trade_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y%m%d')
        # cursor.execute("select * from daily where ts_code='{}' and trade_date='{}'".format(ts_code, trade_date))
        stock_basic = cursor.fetchall()
        if stock_basic:
            common_row.append(stock_basic[0][0])
            common_row.append(stock_basic[0][1])
            common_row.append(stock_basic[0][2])
            common_row.append(stock_basic[0][3])
            common_row.append(stock_basic[0][4])
            common_row.append(stock_basic[0][5])
            common_row.append(stock_basic[0][6])
            common_row.append(stock_basic[0][7])
            common_row.append(stock_basic[0][8])
            common_row.append(stock_basic[0][9])
            common_row.append(stock_basic[0][10])
            common_row.append(stock_basic[0][11])
            common_row.append(stock_basic[0][12])
            common_row.append(stock_basic[0][13])
        else:
            logging.info("[{}] 获取stock_basic值为空: {}".format(loopCount, ts_code))
            continue
        for i in range(2, 2000):
            skip = False
            cur_row = []
            cur_row.extend(common_row)
            for j in range(0, 200):
                shift = i+j
                trade_date = (datetime.datetime.now() - datetime.timedelta(days=shift)).strftime('%Y%m%d')
                cursor.execute("select exchange,cal_date,is_open from `trade_cal` where cal_date='{}' ".format(trade_date))
                trade_cal = cursor.fetchall()
                if trade_cal:
                    cur_row.append(trade_cal[0][0])
                    cur_row.append(trade_cal[0][1])
                    cur_row.append(trade_cal[0][2])
                else:
                    skip = True
                    logging.info("[{}] cal_date: {} {}".format(loopCount, ts_code, trade_date))
            for j in range(0, 200):
                shift = i+j
                trade_date = (datetime.datetime.now() - datetime.timedelta(days=shift)).strftime('%Y%m%d')
                cursor.execute("select `ts_code`,`trade_date`,`open`,`high`,`low`,`close`,`pre_close`,`change`,`pct_chg`,`vol`,`amount` from daily where ts_code='{}' and trade_date='{}'".format(ts_code, trade_date))
                daily = cursor.fetchall()
                if daily:
                    cur_row.append(daily[0][1])
                    cur_row.append(daily[0][2])
                    cur_row.append(daily[0][3])
                    cur_row.append(daily[0][4])
                    cur_row.append(daily[0][5])
                    cur_row.append(daily[0][6])
                    cur_row.append(daily[0][7])
                    cur_row.append(daily[0][8])
                    cur_row.append(daily[0][9])
                    cur_row.append(daily[0][10])
                else:
                    # skip = True
                    cur_row.append('')
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    logging.debug("[{}] 获取历史daily值为空: {} {}".format(loopCount, ts_code, trade_date))
            # 目标值
            shift = i-1
            trade_date = (datetime.datetime.now() - datetime.timedelta(days=shift)).strftime('%Y%m%d')
            cursor.execute("select `ts_code`,`trade_date`,`open`,`high`,`low`,`close`,`pre_close`,`change`,`pct_chg`,`vol`,`amount` from daily where ts_code='{}' and trade_date='{}'".format(ts_code, trade_date))
            daily = cursor.fetchall()
            if daily:
                cur_row.append(daily[0][5])
            else:
                skip = True
                logging.debug("[{}] 获取目标值为空: {} {} {}".format(loopCount, ts_code, i, shift))

            if skip:
                continue
            train_row = pd.DataFrame(np.expand_dims(cur_row, axis=0))
            if train_data_header:
                train_row.to_csv(output_data_dir+"train.csv", sep=',', index=False, mode='w', header=True)
                train_data_header = False
            else:
                train_row.to_csv(output_data_dir+"train.csv", sep=',', index=False, mode='a', header=False)
        logging.info("[{}] finished: {}".format(loopCount, ts_code))

def generate_predict_data():
    train_data_header = True
    for ts_code in stock_pool:
        common_row = []
        cursor.execute("select ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs from `stock_basic` where ts_code='{}' ".format(ts_code))
        # trade_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y%m%d')
        # cursor.execute("select * from daily where ts_code='{}' and trade_date='{}'".format(ts_code, trade_date))
        stock_basic = cursor.fetchall()
        if stock_basic:
            common_row.append(stock_basic[0][0])
            common_row.append(stock_basic[0][1])
            common_row.append(stock_basic[0][2])
            common_row.append(stock_basic[0][3])
            common_row.append(stock_basic[0][4])
            common_row.append(stock_basic[0][5])
            common_row.append(stock_basic[0][6])
            common_row.append(stock_basic[0][7])
            common_row.append(stock_basic[0][8])
            common_row.append(stock_basic[0][9])
            common_row.append(stock_basic[0][10])
            common_row.append(stock_basic[0][11])
            common_row.append(stock_basic[0][12])
            common_row.append(stock_basic[0][13])
        else:
            logging.info("[{}] 获取stock_basic值为空: {}".format(loopCount, ts_code))
            continue
        for i in range(1, 2):
            skip = False
            cur_row = []
            cur_row.extend(common_row)
            for j in range(0, 200):
                shift = i+j
                trade_date = (datetime.datetime.now() - datetime.timedelta(days=shift)).strftime('%Y%m%d')
                cursor.execute("select exchange,cal_date,is_open from `trade_cal` where cal_date='{}' ".format(trade_date))
                trade_cal = cursor.fetchall()
                if trade_cal:
                    cur_row.append(trade_cal[0][0])
                    cur_row.append(trade_cal[0][1])
                    cur_row.append(trade_cal[0][2])
                else:
                    skip = True
                    logging.info("[{}] cal_date: {} {}".format(loopCount, ts_code, trade_date))
            for j in range(0, 200):
                shift = i+j
                trade_date = (datetime.datetime.now() - datetime.timedelta(days=shift)).strftime('%Y%m%d')
                cursor.execute("select `ts_code`,`trade_date`,`open`,`high`,`low`,`close`,`pre_close`,`change`,`pct_chg`,`vol`,`amount` from daily where ts_code='{}' and trade_date='{}'".format(ts_code, trade_date))
                daily = cursor.fetchall()
                if daily:
                    cur_row.append(daily[0][1])
                    cur_row.append(daily[0][2])
                    cur_row.append(daily[0][3])
                    cur_row.append(daily[0][4])
                    cur_row.append(daily[0][5])
                    cur_row.append(daily[0][6])
                    cur_row.append(daily[0][7])
                    cur_row.append(daily[0][8])
                    cur_row.append(daily[0][9])
                    cur_row.append(daily[0][10])
                else:
                    # skip = True
                    cur_row.append('')
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    cur_row.append(0.0)
                    logging.debug("[{}] 获取历史daily值为空: {} {}".format(loopCount, ts_code, trade_date))
            if skip:
                continue
            train_row = pd.DataFrame(np.expand_dims(cur_row, axis=0))
            if train_data_header:
                train_row.to_csv(output_data_dir+"predict.csv", sep=',', index=False, mode='w', header=True)
                train_data_header = False
            else:
                train_row.to_csv(output_data_dir+"predict.csv", sep=',', index=False, mode='a', header=False)
        logging.info("[{}] finished: {}".format(loopCount, ts_code))


def generate_T_data():
    #用前面几天的数据预测
    prepare_len = 11
    empty_csv = True
    for ts_code in stock_pool:
        df = pro.daily(ts_code=ts_code)
        c_len = df.shape[0]
        print('generate_T_data: ', ts_code, c_len)

        for i in range(c_len):
            cur_row = []
            if i + prepare_len < c_len:
                cur_row.append(df.loc[i]['low'])
                cur_row.append(df.loc[i]['high'])
                for j in range(1, prepare_len+1):
                    index = i + j
                    cur_row.append(df.loc[index]['ts_code'])
                    cur_row.append(df.loc[index]['trade_date'])
                    cur_row.append(df.loc[index]['open'])
                    cur_row.append(df.loc[index]['high'])
                    cur_row.append(df.loc[index]['low'])
                    cur_row.append(df.loc[index]['close'])
                    cur_row.append(df.loc[index]['pre_close'])
                    cur_row.append(df.loc[index]['change'])
                    cur_row.append(df.loc[index]['pct_chg'])
                    cur_row.append(df.loc[index]['vol'])
                    cur_row.append(df.loc[index]['amount'])
                train_row = pd.DataFrame(np.expand_dims(cur_row, axis=0))
                if empty_csv:
                    train_row.to_csv(output_data_dir+"train.csv", sep=',', index=False, mode='w', header=True)
                    empty_csv = False
                else:
                    train_row.to_csv(output_data_dir+"train.csv", sep=',', index=False, mode='a', header=False)

    empty_csv = True
    for ts_code in stock_pool:
        df = pro.daily(ts_code=ts_code)
        c_len = df.shape[0]
        print('generate_T_data predict: ', ts_code, c_len)

        cur_row = []
        if prepare_len < c_len:
            for j in range(1, prepare_len+1):
                index = j
                cur_row.append(df.loc[index]['ts_code'])
                cur_row.append(df.loc[index]['trade_date'])
                cur_row.append(df.loc[index]['open'])
                cur_row.append(df.loc[index]['high'])
                cur_row.append(df.loc[index]['low'])
                cur_row.append(df.loc[index]['close'])
                cur_row.append(df.loc[index]['pre_close'])
                cur_row.append(df.loc[index]['change'])
                cur_row.append(df.loc[index]['pct_chg'])
                cur_row.append(df.loc[index]['vol'])
                cur_row.append(df.loc[index]['amount'])
            train_row = pd.DataFrame(np.expand_dims(cur_row, axis=0))
            if empty_csv:
                train_row.to_csv(output_data_dir+"predict.csv", sep=',', index=False, mode='w', header=True)
                empty_csv = False
            else:
                train_row.to_csv(output_data_dir+"predict.csv", sep=',', index=False, mode='a', header=False)
    print('generate_T_data Finished!')


# 设定获取日线行情的初始日期和终止日期，其中终止日期设定为昨天。
start_dt = '20200206'
time_temp = datetime.datetime.now() - datetime.timedelta(days=10)
end_dt = time_temp.strftime('%Y%m%d')
# 设定需要获取数据的股票池
stock_pool = ['600036.SH','000001.SZ','002142.SZ','002807.SZ','002839.SZ','002936.SZ','002948.SZ','002958.SZ','002966.SZ','600000.SH','600015.SH','600016.SH','600908.SH','600919.SH','600926.SH','600928.SH','601009.SH','601077.SH','601128.SH','601166.SH','601169.SH','601229.SH','601288.SH','601328.SH','601398.SH','601577.SH','601658.SH','601818.SH','601838.SH','601860.SH','601916.SH','601939.SH','601988.SH','601997.SH','601998.SH','603323.SH']
#数据目录
data_dir = '/home/dream/workspace/stock/'
output_data_dir = '/home/dream/workspace/stock/'
log_path = '/home/dream/workspace/stock/all.log'
# 建立数据库连接
db = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='stock', charset='utf8')
db.autocommit(1)
cursor = db.cursor()

# 设置tushare pro的token并获取连接
#ts.set_token('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
pro = ts.pro_api()


logging.basicConfig(format="%(asctime)s,%(msecs)3d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)

maxLoopCount = 100
loopCount = 0
try:
    while loopCount < maxLoopCount:
        try:
            # download_stock_data()
            # generate_train_data()
            generate_T_data()


            #     daily = pd.concat([daily,daily1],ignore_index=True)
            # pd.DataFrame(daily).to_csv(data_dir+'日线行情.csv', index=False)
            # df2mysql(conn=db, cursor=cursor, db_name='stock', table_name='daily' , df=daily, uk='ts_code,trade_date')
            #
            # image_input = ak.ImageInput()
            # image_output = ak.ImageBlock()(image_input)
            # text_input = ak.TextInput()
            # text_output = ak.TextBlock()(text_input)
            # output = ak.Merge()([image_output, text_output])
            # classification_output = ak.ClassificationHead()(output)
            # regression_output = ak.RegressionHead()(output)
            # ak.AutoModel(
            #     inputs=[image_input, text_input],
            #     outputs=[classification_output, regression_output]
            # )
            logging.info('[{}] 程序正常结束'.format(loopCount))
            loopCount = maxLoopCount
        except Exception as e:
            loopCount = loopCount + 1
            logging.error('[{}] 发生了异常'.format(loopCount))
            logging.exception(e)
finally:
    logging.info('[{}] All Finished!'.format(loopCount))
    cursor.close()
    db.close()


