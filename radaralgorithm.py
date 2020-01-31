import pandas as pd
import pymysql
import time
import math
from collections import Counter
import numpy as np
import pandas as pd

# 计算当前是第几周
def Week(t_time):
    tl = time.localtime(t_time)
    b = time.strftime("%Y-%m-%d %H:%M:%S", tl)
    week = pd.to_datetime(b).strftime("%W")
    return int(week)

# 获得当前年月
def add_year_mon(t_time):
    t2 = time.localtime(t_time)
    b = time.strftime("%Y%m%d %H:%M:%S", t2)
    return int(b[0:6])

#获取当前日
def Add_day(t_time):
    t2 = time.localtime(t_time)
    b = time.strftime("%Y%m%d %H:%M:%S", t2)
    return int(b[7:8])






# 查询公司简称
def Search_company_short_name(car_vh,connect):
    # connect = pymysql.connect(host='119.23.109.128', port=3306, user='root', passwd='Sues5220.', db="diagnosis",charset='utf8')
    sql = "SELECT company_short_name  FROM t_car_startend WHERE car_vh = '%s' ORDER BY id DESC LIMIT 1;" % (car_vh)
    company_short_name = pd.read_sql(sql,connect)['company_short_name'][0]
    return company_short_name

# 马非凡（许志宇充电时间段评分）
class Radar_algorithm(object):
    def __init__(self, conn):
        self.conn = conn
    # 1、快充频率
    def cal_char_num(self, start_time, end_time, car_vh):
        try:
            #             时间都是时间戳形式，精确到秒级
            sql = "SELECT data_time_end FROM t_car_startend WHERE data_time_start BETWEEN '%s' AND '%s'and car_vh = '%s';" % (
            start_time,
            end_time, car_vh)
            a = pd.read_sql(sql,self.conn)
            number_of_charges = len(a)
            return number_of_charges
        except Exception as e:
            raise e

    def cal_chrg_dc_fery_score(self, chrg_num):
        if chrg_num < 5:
            score = 10
        elif chrg_num >= 5 and chrg_num < 8:
            score = 8
        elif chrg_num >= 8 and chrg_num < 10:
            score = 4
        else:
            score = 2
        return score

        # 2、过放电频率

    #2、过放电频率

    def cal_chrg_num_under(self, start_time, end_time, car_vh):
        try:
            sql = "SELECT id FROM t_car_startend WHERE data_time_start BETWEEN '%s' AND '%s'and car_vh = '%s';" % (start_time,end_time,car_vh)
            id_list = pd.read_sql(sql, self.conn)
            number_of_charges = len(id_list)
            chrg_num_under = 0
            for i in id_list['id']:
                sql_runtime = "SELECT bat_soc FROM t_car_runtime WHERE car_startend_id = %s ORDER BY id ASC LIMIT 1; " % i
                soc_start = pd.read_sql(sql_runtime, self.conn)['bat_soc'][0]
                if soc_start <= 20:
                    chrg_num_under = chrg_num_under + 1
            if number_of_charges==0:
                dsch_over_fery=0
            else:
                dsch_over_fery = chrg_num_under / number_of_charges
            return dsch_over_fery
        except Exception as e:
            raise e

    def cal_dsch_over_fery_score(self, dsch_over_fery):
        m = dsch_over_fery
        if m < 0.2:
            score = 10
        elif m >= 0.2 and m < 0.3:
            score = 8
        elif m >= 0.3 and m < 0.4:
            score = 6
        elif m >= 0.4 and m < 0.6:
            score = 4
        else:
            score = 2
        return score

    # 3放电深度
    def cal_dsch_dod_avg(self, start_time, end_time, car_vh):
        try:
            sql = "SELECT id FROM t_car_startend WHERE data_time_start BETWEEN '%s' AND '%s'and car_vh = '%s';" % (
            start_time, end_time, car_vh)
            id_list = pd.read_sql(sql, self.conn)
            number_of_charges = len(id_list)
            soc_add = 0
            for i in id_list['id']:
                sql_runtime = "SELECT bat_soc FROM t_car_runtime WHERE car_startend_id = %s ORDER BY id ASC LIMIT 1; " % i
                soc_start = pd.read_sql(sql_runtime, self.conn)['bat_soc'][0]
                soc_add = soc_add + soc_start
            if number_of_charges == 0:
                dsch_dod_avg = 0
            else:
                dsch_dod_avg = 100 - (soc_add / number_of_charges)
            return dsch_dod_avg
        except Exception as e:
            raise e

    def cal_dsch_dod_score(self, dsch_dod_avg):
        m = 100 - dsch_dod_avg
        if m < 10:
            score = 0
        elif m >= 10 and m < 20:
            score = 0.2
        elif m >= 20 and m < 30:
            score = 0.6
        elif m >= 30 and m < 70:
            score = 1
        elif m >= 70 and m < 80:
            score = 0.8
        elif m >= 80 and m < 90:
            score = 0.5
        else:
            score = 0.2
        return score
    # 分时电价评分

    #  5正常充电结束频率
    def cal_chrg_end_fery(self, start_time, end_time, car_vh):
        try:
            #             时间都是时间戳形式，精确到秒级
            sql = "SELECT data_time_end FROM `t_car_startend`WHERE data_time_start BETWEEN '%s' AND '%s'and car_vh = '%s';" % (
            start_time, end_time, car_vh)
            a = pd.read_sql(sql, self.conn)
            chrg_normal_end_num = 0
            for i in range(0, len(a) - 1):
                if math.isnan(a['data_time_end'][i + 1]) == False:
                    chrg_normal_end_num = chrg_normal_end_num + 1
            number_of_charges = self.cal_char_num(start_time, end_time, car_vh)
            if number_of_charges == 0:
                chrg_end_fery = 0
            else:
                chrg_end_fery = chrg_normal_end_num / number_of_charges
            return chrg_end_fery
        except Exception as e:
            raise e

    def cal_chrg_end_fery_score(self, dsch_over_fery):
        m = dsch_over_fery
        if m < 0.2:
            score = 2
        elif m >= 0.2 and m < 0.3:
            score = 4
        elif m >= 0.3 and m < 0.4:
            score = 6
        elif m >= 0.4 and m < 0.6:
            score = 8
        else:
            score = 10
        return score
    # 许志宇充电时间段评分
    def shijianduan(self, m, n, p):
        try:
            sql = "SELECT id,car_vh,data_time_start,data_time_end FROM `t_car_startend`;"
            a = pd.read_sql(sql, self.conn)
        except Exception as e:
            print("错误信息：", e)
        c = a[m <= a.data_time_start]
        c = c[c.data_time_start <= n]
        return c[c.car_vh == p]

    def dianjia(self, m, n):
        time_local = time.localtime(m)
        dt1 = time.strftime("%H:%M:%S", time_local)
        dt2 = str(dt1)
        m = int(dt2.split(":")[0])*3600+int(dt2.split(":")[1])*60+int(dt2.split(":")[2])
        time_local = time.localtime(n)
        dt4 = time.strftime("%H:%M:%S", time_local)
        dt4 = str(dt4)
        n = int(dt4.split(":")[0])*3600+int(dt4.split(":")[1])*60+int(dt4.split(":")[2])
        if 0 <= int(dt2.split(":")[0]) < 7:
            if 0 <= int(dt4.split(":")[0]) < 7:
                chrg_peak_time_rate = 0
                chrg_noun_time_rate = 0
                chrg_vally_time_rate = 1
                chrg_time_score = 10 * (n - m) / 25200
            elif 7 <= int(dt4.split(":")[0]) < 10:
                chrg_peak_time_rate = 0
                chrg_noun_time_rate = (n-25200)/(n-m)
                chrg_vally_time_rate = (25200-m)/(n-m)
                chrg_time_score = 10 * (25200 - m) / 25200 + 8.5 * (n - 25200) / 10800
            elif 10 <= int(dt4.split(":")[0]) < 12:
                chrg_peak_time_rate = (n-36000) / (n - m)
                chrg_noun_time_rate = 10800 / (n - m)
                chrg_vally_time_rate = (25200 - m) / (n - m)
                chrg_time_score = 10 * (25200 - m) / 25200 + 8.5 + 7 * (n - 36000) / 7200
            elif 12 <= int(dt4.split(":")[0]) < 22:
                chrg_peak_time_rate = 7200 / (n - m)
                chrg_noun_time_rate = 10800 / (n - m) + (n-43200)/(n-m)
                chrg_vally_time_rate = (25200 - m) / (n - m)
                chrg_time_score = 10 * (25200 - m) / 25200 + 8.5 + 7 + 8.5 * (n - 43200) / 36000
            elif 22 <= int(dt4.split(":")[0]) < 23:
                chrg_peak_time_rate = 7200 / (n - m)+(n-79200)/(n-m)
                chrg_noun_time_rate = 10800 / (n - m)+36000/(n-m)
                chrg_vally_time_rate = (25200 - m) / (n - m)
                chrg_time_score = 10 * (25200 - m) / 25200 + 8.5 + 7 + 8.5 + 7 * (n - 79200) / 3600
            else:
                chrg_peak_time_rate = 7200 / (n - m) + 3600 / (n - m)
                chrg_noun_time_rate = 10800 / (n - m)+36000/(n-m)
                chrg_vally_time_rate = (25200 - m) / (n - m)+(n-82800)/(n-m)
                chrg_time_score = 10 * (25200 - m) / 25200 + 8.5 + 7 + 8.5 + 7 + 10 * (n - 82800) / 3600
            return chrg_peak_time_rate, chrg_noun_time_rate, chrg_vally_time_rate, chrg_time_score
        elif 23 <= int(dt2.split(":")[0]) <= 24:
            chrg_peak_time_rate = 0
            chrg_noun_time_rate = 0
            chrg_vally_time_rate = 1
            chrg_time_score = 10 + 8.5 + 7 + 8.5 + 7 + 10 * (n - 82800) / 3600
            return chrg_peak_time_rate, chrg_noun_time_rate, chrg_vally_time_rate, chrg_time_score
        elif 10 <= int(dt2.split(":")[0]) <= 12:
            if 10 <= int(dt4.split(":")[0]) < 12:
                chrg_peak_time_rate = 1
                chrg_noun_time_rate = 0
                chrg_vally_time_rate = 0
                chrg_time_score = 7 * (n - m) / 7200
            elif 12 <= int(dt4.split(":")[0]) < 22:
                chrg_peak_time_rate = (43200-m) / (n - m)
                chrg_noun_time_rate = (n-43200)/(n-m)
                chrg_vally_time_rate = 0
                chrg_time_score = 7 * (43200 - m) / 7200 + 8.5 * (n - 43200) / 36000
            elif 22 <= int(dt4.split(":")[0]) < 23:
                chrg_peak_time_rate = (43200-m) / (n - m)+(n-79200)/(n-m)
                chrg_noun_time_rate = 36000/(n-m)
                chrg_vally_time_rate = 0
                chrg_time_score = 7 * (43200 - m) / 7200 + 8.5 + 7 * (n - 79200) / 3600
            else:
                chrg_peak_time_rate = (43200 - m) / (n - m) + 3600 / (n - m)
                chrg_noun_time_rate = 36000 / (n - m)
                chrg_vally_time_rate = (n-82800)/(n-m)
                chrg_time_score = 7 * (43200 - m) / 7200 + 8.5 + 7 + 10 * (n - 82800) / 3600
            return chrg_peak_time_rate, chrg_noun_time_rate, chrg_vally_time_rate, chrg_time_score
        elif 22 <= int(dt2.split(":")[0]) < 23:
            if 22 <= int(dt4.split(":")[0]) < 23:
                chrg_peak_time_rate = 1
                chrg_noun_time_rate = 0
                chrg_vally_time_rate = 0
                chrg_time_score = 7 * (n - m) / 3600
            else:
                chrg_peak_time_rate = (82800 - m) / (n - m)
                chrg_noun_time_rate = 0
                chrg_vally_time_rate = (n-82800)/(n-m)
                chrg_time_score = 7 * (82800 - m) / 3600 + 10 * (n - 82800) / 3600
            return chrg_peak_time_rate, chrg_noun_time_rate, chrg_vally_time_rate, chrg_time_score
        elif 7 <= int(dt2.split(":")[0]) < 10:
            if 7 <= int(dt4.split(":")[0]) < 10:
                chrg_peak_time_rate = 0
                chrg_noun_time_rate = 1
                chrg_vally_time_rate = 0
                chrg_time_score = 8.5 * (n - m) / 10800
            elif 10 <= int(dt4.split(":")[0]) < 12:
                chrg_peak_time_rate = (n - 36000) / (n - m)
                chrg_noun_time_rate = (36000-m) / (n - m)
                chrg_vally_time_rate = 0
                chrg_time_score = 8.5 * (36000 - m) / 10800 + 7 * (n - 36000) / 7200
            elif 12 <= int(dt4.split(":")[0]) < 22:
                chrg_peak_time_rate = 7200 / (n - m)
                chrg_noun_time_rate = (36000-m)/(n-m)+(n-43200) / (n - m)
                chrg_vally_time_rate = 0
                chrg_time_score = 8.5 * (36000 - m) / 10800 + 7 + 8.5 * (n - 43200) / 36000
            elif 22 <= int(dt4.split(":")[0]) < 23:
                chrg_peak_time_rate = 7200 / (n - m) + (n - 79200) / (n - m)
                chrg_noun_time_rate = (36000-m) / (n - m)+36000/(n-m)
                chrg_vally_time_rate = 0
                chrg_time_score = 8.5 * (36000 - m) / 10800 + 7 + 8.5 + 7 * (n - 79200) / 3600
            else:
                chrg_peak_time_rate = 7200 / (n - m) + 3600 / (n - m)
                chrg_noun_time_rate = (36000-m) / (n - m) + 36000/(n-m)
                chrg_vally_time_rate = (n-82800)/(n-m)
                chrg_time_score = 8.5 * (36000 - m) / 10800 + 7 + 8.5 + 7 + 10 * (n - 82800) / 3600
            return chrg_peak_time_rate, chrg_noun_time_rate, chrg_vally_time_rate, chrg_time_score
        else:
            if 12 <= int(dt4.split(":")[0]) < 22:
                chrg_peak_time_rate = 0
                chrg_noun_time_rate = 1
                chrg_vally_time_rate = 0
                chrg_time_score = 8.5 * (n - m) / 36000
            elif 22 <= int(dt4.split(":")[0]) < 23:
                chrg_peak_time_rate = (n - 79200) / (n - m)
                chrg_noun_time_rate = (79200-m) / (n - m)
                chrg_vally_time_rate = 0
                chrg_time_score = 8.5 * (79200 - m) / 36000 + 7 * (n - 79200) / 3600
            else:
                chrg_peak_time_rate = 3600 / (n - m)
                chrg_noun_time_rate = (79200-m) / (n - m)
                chrg_vally_time_rate = (n-82800)/(n-m)
                chrg_time_score = 8.5 * (79200 - m) / 36000 + 7 + 10 * (n - 82800) / 3600
            return chrg_peak_time_rate, chrg_noun_time_rate, chrg_vally_time_rate, chrg_time_score


# 刘东
class Temp_con(object):
    def __init__(self,conn):
        self.conn = conn
    def temp_num(self,car,start_time,end_time):
        temp_noun_num = 8  # 当总共出现的序号数不大于temp_noun_num个时，全部更新显示在界面上
        temp_noun_frey_total = 0.8  # 当总共出现的序号数大于temp_noun_num个时，对各序号频率按降序排列，对排在前面的序号频率累加，对排列在前面的temp_noun_num个的累加频率大于temp_noun_frey_total时，后面的划分为其他中去更新显示在界面上。
        # conn = pymysql.connect(host='119.23.109.128', port=3306, user='root', passwd='Sues5220.', charset='utf8')
        try:
            sql = "SELECT temp_no_max FROM t_car_runtime WHERE( car_vh = '%s' and  data_time BETWEEN '%s' AND '%s')" % (car,start_time, end_time)
            data = pd.read_sql(sql, self.conn)
            data = list(data['temp_no_max'].dropna())  # 转换成列表
            total_num = len(data)  # data中数据的个数
            total_temp_noun_num = []  # temp_noun_num数据个数
            court = Counter(data)
            cell_temp_max_no = []  # 计算后并排序的编号
            court = sorted(court.items(), key=lambda x: x[1], reverse=True)  # 将字典转为列表并进行降序排序
            # 对出现的电池序号个数进行判断
            if len(court) <= temp_noun_num:
                for i in range(0, len(court)):
                    cell_temp_max_no.append(int(court[i][0]))
                return cell_temp_max_no
            if len(court) > temp_noun_num:
                for i in range(0, temp_noun_num):
                    total_temp_noun_num.append(court[i][1])
                total_temp_noun_num_frey = sum(np.array(total_temp_noun_num)) / total_num  # num前的频率
                for i in range(0, temp_noun_num):
                    if total_temp_noun_num_frey >= temp_noun_frey_total:
                        cell_temp_max_no.append(int(court[i][0]))
                cell_temp_max_no.append('其他')
        except Exception as e:
            raise e
    def temp_frey(self,car,start_time,end_time):
        temp_noun_num = 8  # 当总共出现的序号数不大于temp_noun_num个时，全部更新显示在界面上
        temp_noun_frey_total = 0.8  # 当总共出现的序号数大于temp_noun_num个时，对各序号频率按降序排列，对排在前面的序号频率累加，对排列在前面的temp_noun_num个的累加频率大于temp_noun_frey_total时，后面的划分为其他中去更新显示在界面上
        # conn = pymysql.connect(host='119.23.109.128', port=3306, user='root', passwd='Sues5220.', charset='utf8')
        try:
            sql = "SELECT temp_no_max FROM t_car_runtime WHERE( car_vh = '%s' and  data_time BETWEEN '%s' AND '%s')" % (car, start_time, end_time)
            data = pd.read_sql(sql, self.conn)
            data = list(data['temp_no_max'].dropna())  # 转换成列表
            total_num = len(data)  # data中数据的个数
            total_temp_noun_num = []  # temp_noun_num数据个数
            court = Counter(data)
            temp_max_frey_no = []  # 计算后并排序的频率
            court = sorted(court.items(), key=lambda x: x[1], reverse=True)  # 将字典转为列表并进行降序排序
            # 对出现的电池序号个数进行判断
            if len(court) <= temp_noun_num:
                for i in range(0, len(court)):
                    temp_max_frey_no.append(round(court[i][1] / total_num, 2))
                return temp_max_frey_no
            if len(court) > temp_noun_num:
                for i in range(0, temp_noun_num):
                    total_temp_noun_num.append(court[i][1])
                total_temp_noun_num_frey = sum(np.array(total_temp_noun_num)) / total_num  # num前的频率
                for i in range(0, temp_noun_num):
                    if total_temp_noun_num_frey >= temp_noun_frey_total:
                        temp_max_frey_no.append(round(court[i][1] / total_num,2))
                temp_max_frey_no.append(round(1 - total_temp_noun_num_frey,2))
            return temp_max_frey_no
        except Exception as e:
            raise e
    def temp_unifomity(self,car,start_time,end_time):
        temp_noun_num = 8  # 当总共出现的序号数不大于temp_noun_num个时，全部更新显示在界面上
        temp_noun_frey_total = 0.8  # 当总共出现的序号数大于temp_noun_num个时，对各序号频率按降序排列，对排在前面的序号频率累加，对排列在前面的temp_noun_num个的累加频率大于temp_noun_frey_total时，后面的划分为其他中去更新显示在界面上。
        temp_noun_frey1 = 0.8
        temp_noun_frey2 = 0.5
        temp_noun_frey3 = 0.3
        temp_noun_frey4 = 0.1
        temp_noun_before = 3  # 当最高频率中排前temp_noun_before的序号频率总和超过temp_noun_frey1,temp_noun_frey2,temp_noun_frey3,temp_noun_frey4时，判断一致性分别为差、一般、中、良好；
        # conn = pymysql.connect(host='119.23.109.128', port=3306, user='root', passwd='Sues5220.', charset='utf8')
        try:
            sql = "SELECT temp_no_max FROM t_car_runtime WHERE( car_vh = '%s' and  data_time BETWEEN '%s' AND '%s')" % (car, start_time, end_time)
            data = pd.read_sql(sql, self.conn)
            data = list(data['temp_no_max'].dropna())  # 转换成列表
            total_num = len(data)  # data中数据的个数
            total_temp_noun_before = []  # temp_noun_before数据个数
            total_temp_noun_num = []  # temp_noun_num数据个数
            court = Counter(data)
            temp_max_frey_no = []
            court = sorted(court.items(), key=lambda x: x[1], reverse=True)  # 将字典转为列表并进行降序排序
            # 对出现的电池序号个数进行判断
            if len(court) <= temp_noun_num:
                for i in range(0, len(court)):
                    temp_max_frey_no.append(round(court[i][1] / total_num, 2))
            if len(court) > temp_noun_num:
                for i in range(0, temp_noun_num):
                    total_temp_noun_num.append(court[i][1])
                total_temp_noun_num_frey = sum(np.array(total_temp_noun_num)) / total_num  # num前的频率
                for i in range(0, temp_noun_num):
                    if total_temp_noun_num_frey >= temp_noun_frey_total:
                        temp_max_frey_no.append(round(court[i][1] / total_num, 2))
                temp_max_frey_no.append(round(1 - total_temp_noun_num_frey, 2))
            if len(court) <= temp_noun_before:
                temp_consiy_eval_1 = u'差'
            if len(court) > temp_noun_before:
                for i in range(0, temp_noun_before):
                    total_temp_noun_before.append(court[i][1])
                total_temp_noun_before_frey = sum(np.array(total_temp_noun_before)) / total_num  # before前的数据
                if total_temp_noun_before_frey >= temp_noun_frey1:
                    temp_consiy_eval_1 = u'差'
                if temp_noun_frey1 >= total_temp_noun_before_frey >= temp_noun_frey2:
                    temp_consiy_eval_1 = u'一般'
                if temp_noun_frey2 >= total_temp_noun_before_frey >= temp_noun_frey3:
                    temp_consiy_eval_1 = u'中'
                if temp_noun_frey3 >= total_temp_noun_before_frey >= temp_noun_frey4:
                    temp_consiy_eval_1 = u'良好'
            return temp_consiy_eval_1
        except Exception as e:
            raise e

# 王一全
history_cell_volt_max_no = []  # 编号历史数据 若不满足条件时 界面不更新
history_volt_max_frey_no = []   # 频率历史数据

class Volt_unifomity(object):
    """类：短板电池统计与电压一致性判断,该类需要的参数：conn即数据库地址，car_vh即车辆标示符，start_time即算法起始时间，end_time即算法截止时间"""
    def __init__(self, conn, car_vh, start_time, end_time):
        self.conn = conn
        self.car_vh = car_vh
        self.start_time = start_time
        self.end_time = end_time

    """battery_keys方法：短板电池统计,统计各单体序号，并return（模组序号列表，单体序号列表）"""
    def battery_keys(self):
        try:
        # sql语句，视情况而定，根据自己的业务需要
            sql = "SELECT module_volt_no_max, cell_volt_no_max FROM diagnosis.t_car_runtime WHERE car_vh = '%s' AND data_time BETWEEN %d AND %d" % (self.car_vh, self.start_time, self.end_time)
            # read_sql传入参数sql语句，目标数据库，得到对应表
            data = pd.read_sql(sql, self.conn)
            module_data = list(data['module_volt_no_max'].dropna())  # 删除数据库中的Null值，并转化为列表
            cell_data = list(data['cell_volt_no_max'].dropna())
            combination_data = []
            for com in range(len(module_data)):
                if len(str(module_data[com])) == 1 and len(str(cell_data[com])) == 1:
                    combination_data.append('0'+str(module_data[com])+'0'+str(cell_data[com]))
                if len(str(module_data[com])) == 1 and len(str(cell_data[com])) == 2:
                    combination_data.append('0'+str(module_data[com])+str(cell_data[com]))
                if len(str(module_data[com])) == 2 and len(str(cell_data[com])) == 1:
                    combination_data.append(str(module_data[com]) + '0' + str(cell_data[com]))
                if len(str(module_data[com])) == 2 and len(str(cell_data[com])) == 2:
                    combination_data.append(str(module_data[com]) + str(cell_data[com]))
            # print(combination_data)
            set1 = set(combination_data)  # 删除重复出现的编号，统计共有几个单体出现过最高电压
            dict_unsorted = {}  # 未排序的字典 key：编号 value：出现的次数
            dict_calculated = {}  # 排序并计算后的字典  key:编号  value:频率
            volt_max_no = []   # 计算后并排序的组合编号
            module_volt_max_no = []   # 计算后并排序的箱号编号
            cell_volt_max_no = []  # 计算后并排序的单体编号
            grades = []   # 所有频率
            key_nums = []  # 所有组合编号
            # print(set1)
            for item in set1:
                dict_unsorted.update({item: combination_data.count(item)})  # 将提取出的数据转成字典格式 key:编号 value:出现次数
            # print(dict_unsorted)
            list_sorted = sorted(dict_unsorted.items(), key=lambda x: x[1], reverse=True)  # 将字典转为列表并进行排序
            # print(list_sorted)
            for num in range(len(list_sorted)):
                key = list_sorted[num][0]
                dict_calculated[key] = list_sorted[num][1] / sum(dict_unsorted.values())  # 将各编号出现次数计算为频率
            list_calculated = sorted(dict_calculated.items(), key=lambda x: x[1], reverse=True)  # 转成列表格式
            # print(list_calculated)
            for grade in range(len(list_calculated)):
                key_nums.append(list_calculated[grade][0])
                grades.append(list_calculated[grade][1])
            if len(list_sorted) <= 10:  # 短板电池统计，判断总共出现的序号是否大于num_frequency,不大于则全部显示
                for num_1 in range(len(list_sorted)):
                    volt_max_no.append(list_sorted[num_1][0])
                if len(volt_max_no) < 10:
                    for add_num in range(10-len(volt_max_no)):
                        volt_max_no.append(-1)
            else:  # 总共出现的序号若大于num_frequency,进一步判断
                if sum(grades[:(10 - 1)]) > 0.8:  # 若前（num_frequency-1）个的频率和大于volt_noun_frey_total，则第num_frequency个显示为‘其他’
                    volt_max_no = key_nums[:(10 - 1)]
                    volt_max_no.append('其他')
                    if len(volt_max_no) < 10:
                        for add_num in range(10 - len(volt_max_no)):
                            volt_max_no.append(-1)
                else:  # 若不满足条件则不更新
                    volt_max_no = history_cell_volt_max_no
            # print(volt_max_no)
            for divide in range(len(volt_max_no)):
                if volt_max_no[divide] != -1:
                    module_volt_max_no.append(volt_max_no[divide][:2])
                    cell_volt_max_no.append(volt_max_no[divide][-2:])
                if volt_max_no[divide] == -1:
                    module_volt_max_no.append(-1)
                    cell_volt_max_no.append(-1)
            # print(module_volt_max_no)
            # print(cell_volt_max_no)
            return module_volt_max_no, cell_volt_max_no
        except Exception as e:
            print("错误信息：", e)

    """battery_grades方法：电压一致性判断，计算各单体出现的频率，并据此给出一致性判断结果，return频率列表，列表最后一个值为判断结果"""
    def battery_grades(self):
        try:
            # sql语句，视情况而定，根据自己的业务需要
            sql = "SELECT module_volt_no_max, cell_volt_no_max FROM diagnosis.t_car_runtime WHERE car_vh = '%s' AND data_time BETWEEN %d AND %d" % (self.car_vh, self.start_time, self.end_time)
            # read_sql传入参数sql语句，目标数据库，得到对应表
            data = pd.read_sql(sql, self.conn)
            module_data = list(data['module_volt_no_max'].dropna())  # 删除数据库中的Null值，并转化为列表
            cell_data = list(data['cell_volt_no_max'].dropna())
            combination_data = []
            for com in range(len(module_data)):
                if len(str(module_data[com])) == 1 and len(str(cell_data[com])) == 1:
                    combination_data.append('0'+str(module_data[com])+'0'+str(cell_data[com]))
                if len(str(module_data[com])) == 1 and len(str(cell_data[com])) == 2:
                    combination_data.append('0'+str(module_data[com])+str(cell_data[com]))
                if len(str(module_data[com])) == 2 and len(str(cell_data[com])) == 1:
                    combination_data.append(str(module_data[com]) + '0' + str(cell_data[com]))
                if len(str(module_data[com])) == 2 and len(str(cell_data[com])) == 2:
                    combination_data.append(str(module_data[com]) + str(cell_data[com]))
            # print(combination_data)
            set1 = set(combination_data)  # 删除重复出现的编号，统计共有几个单体出现过最高电压
            dict_unsorted = {}  # 未排序的字典 key：编号 value：出现的次数
            dict_calculated = {}  # 排序并计算后的字典  key:编号  value:频率
            # volt_max_no = []  # 计算后并排序的组合编号
            # module_volt_max_no = []   # 计算后并排序的箱号编号
            # cell_volt_max_no = []  # 计算后并排序的单体编号
            grades = []   # 所有频率
            volt_max_frey_no = []
            for item in set1:
                dict_unsorted.update({item: combination_data.count(item)})  # 将提取出的数据转成字典格式 key:编号 value:出现次数
            list_sorted = sorted(dict_unsorted.items(), key=lambda x: x[1], reverse=True)  # 将字典转为列表并进行排序
            # print(list_sorted)
            for num in range(len(list_sorted)):
                key = list_sorted[num][0]
                dict_calculated[key] = list_sorted[num][1] / sum(dict_unsorted.values())  # 将各编号出现次数计算为频率
            list_calculated = sorted(dict_calculated.items(), key=lambda x: x[1], reverse=True)  # 转成列表格式
            for grade in range(len(list_calculated)):
                grades.append(round(list_calculated[grade][1], 2))
            # print(grades)
            if sum(grades[:3]) > 0.9:    # 利用频率排前三的总和进行电压一致性判断
                volt_consiy_eval_thistime = u'差'
            if 0.75 < sum(grades[:3]) <= 0.9:
                volt_consiy_eval_thistime = u'一般'
            if 0.5 < sum(grades[:3]) <= 0.75:
                volt_consiy_eval_thistime = u'中'
            if sum(grades[:3]) <= 0.5:
                volt_consiy_eval_thistime = u'良好'
            # print(volt_consiy_eval_thistime)
            # print(list_calculated)
            if len(list_sorted) <= 10:   # 短板电池统计，判断总共出现的序号是否大于num_frequency,不大于则全部显示
                for num_1 in range(len(list_calculated)):
                    volt_max_frey_no.append(round(list_calculated[num_1][1], 2))
                if len(volt_max_frey_no) < 10:
                    for add_num in range(10-len(volt_max_frey_no)):
                        volt_max_frey_no.append(-1)
                    volt_max_frey_no.append(volt_consiy_eval_thistime)
            else:   # 总共出现的序号若大于num_frequency,进一步判断
                if sum(grades[:(10 - 1)]) > 0.8:  # 若前（num_frequency-1）个的频率和大于volt_noun_frey_total，则第num_frequency个显示为剩余的和
                    volt_max_frey_other = sum(grades[(10 - len(list_calculated) - 1):])
                    volt_max_frey_no = grades[:(10 - 1)]
                    volt_max_frey_no.append(volt_max_frey_other)
                    if len(volt_max_frey_no) < 10:
                        for add_num in range(10 - len(volt_max_frey_no)):
                            volt_max_frey_no.append(-1)
                        volt_max_frey_no.append(volt_consiy_eval_thistime)
                else:  # 若不满足条件则不更新
                    volt_max_frey_no = history_volt_max_frey_no
            # print(volt_max_frey_no)
            return volt_max_frey_no
        except Exception as e:
            print("错误信息：", e)

# 马非凡 电池系统容量变化
class Bat_sys_cap_change(object):
    def __init__(self, conn):
        self.conn = conn
# 某一次充电过程电量跟容量
    def each_chrg_cap_ammeter(self,car_startend_id):
    #     car_startend_id = 2
        sql_select = "SELECT dc_chrg_cap,ammeter_num FROM `t_car_runtime`WHERE car_startend_id=%s;" % (car_startend_id)
        cap = pd.read_sql(sql_select, self.conn)['dc_chrg_cap']
        each_cap = cap[len(cap)-1]-cap[0]
        ammeter = pd.read_sql(sql_select, self.conn)['ammeter_num']
        each_ammeter= ammeter[len(ammeter)-1]-ammeter[0]
        return each_ammeter,each_cap

    def cal_Bat_sys_cap_change(self,start_time,end_time,car_vh):
        sql = "SELECT id FROM `t_car_startend`WHERE data_time_start BETWEEN '%s' AND '%s'and car_vh = '%s'" % (start_time,end_time,car_vh)
        id_list = pd.read_sql(sql, self.conn)['id']
        capacity_bat = 0
        chrg_cap_total = 0
        for i in id_list:
            each_result = self.each_chrg_cap_ammeter(i)
            capacity_bat = capacity_bat+each_result[1]
            chrg_cap_total = chrg_cap_total+each_result[0]
        return capacity_bat,chrg_cap_total
