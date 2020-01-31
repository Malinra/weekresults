from flask import Flask, request
from flask import  json
import pymysql
import time
import pandas as pd
from radaralgorithm import Radar_algorithm
from radaralgorithm import Temp_con
from radaralgorithm import Volt_unifomity
from radaralgorithm import Search_company_short_name
from radaralgorithm import Bat_sys_cap_change
from radaralgorithm import Week
from radaralgorithm import add_year_mon
from radaralgorithm import Add_day

app = Flask(__name__)

@app.route('/weekresults', methods=['POST'])
def register():
    if request.headers['Content-Type'] == 'application/json':
        # 获取到指令，解码
        data = request.json
        # 提取指令三个参数
        car_vh = data['car_vh']
        start_time = data['start_time']
        end_time = data['end_time']

        # # 初始值
        # car_vh = '1G1BL52P7TR115521'
        # start_time = 1529127672
        # end_time = 1530426672

        # 与数据库建立连接
        conn = pymysql.connect(host='119.23.109.128', port=3306, user='root', passwd='Sues5220.', db="diagnosis",
                               charset='utf8')
        # 蜘蛛网图计算的五类指标（马非凡负责，分时电价许志宇负责）
        try:
            # 蜘蛛网图计算的五类指标（马非凡负责，分时电价许志宇负责）
            # try:
            adar_algorithm = Radar_algorithm(conn)
            chrg_dc_fery = adar_algorithm.cal_char_num(start_time, end_time, car_vh)  # 快充频次
            chrg_dc_fery_score = adar_algorithm.cal_chrg_dc_fery_score(chrg_dc_fery)  # 快充频率得分
            dsch_over_fery = adar_algorithm.cal_chrg_num_under(start_time, end_time, car_vh)  # 过放电频率
            dsch_over_fery_score = adar_algorithm.cal_dsch_over_fery_score(dsch_over_fery)  # 过放电频率得分
            dsch_dod_avg = adar_algorithm.cal_dsch_dod_avg(start_time, end_time, car_vh)  # 放电深度
            dsch_dod_score = adar_algorithm.cal_dsch_dod_score(dsch_dod_avg)  # 放电深度得分
            chrg_end_fery = adar_algorithm.cal_chrg_end_fery(start_time, end_time, car_vh)  # 正常充电结束频率
            chrg_end_fery_score = adar_algorithm.cal_chrg_end_fery_score(chrg_end_fery)  # 正常充电结束频率得分
            adar_algorithm_key = 'chrg_dc_fery, chrg_dc_fery_score, dsch_over_fery, dsch_over_fery_score, dsch_dod_avg, dsch_dod_score,chrg_end_fery, chrg_end_fery_score'
            adar_algorithm_value = (
                chrg_dc_fery, round(chrg_dc_fery_score, 2), round(dsch_over_fery, 2), round(dsch_over_fery_score, 2),
                round(dsch_dod_avg, 2), round(dsch_dod_score, 2), round(chrg_end_fery, 2),
                round(chrg_end_fery_score, 2))
            print('马非凡负责', adar_algorithm_value)

            # r为特定车辆，特定时间段内返回Dataframe
            adar_algorithm = Radar_algorithm(conn)
            # r为特定车辆，特定时间段内返回Dataframe
            r = adar_algorithm.shijianduan(1534370554, 1534631344, '1G1BL52P7TR115520')
            chrg_peak_time_rate = []
            chrg_noun_time_rate = []
            chrg_vally_time_rate = []
            chrg_time_score = []
            for index, row in r.iterrows():
                id = row["id"]
                try:
                    sql = "SELECT car_startend_id,car_vh,data_time FROM `t_car_runtime`;"
                    u = pd.read_sql(sql, conn)
                except Exception as e:
                    print("错误信息：", e)
                a1 = u[u.car_startend_id == id]
                time_local = time.localtime(a1.iloc[0][-1])
                dt1 = time.strftime("%Y.%m.%d", time_local)
                time_local = time.localtime(a1.iloc[-1][-1])
                dt2 = time.strftime("%Y.%m.%d", time_local)
                timeArray = time.strptime(dt2, "%Y.%m.%d")
                timeStamp = int(time.mktime(timeArray))
                if dt1 == dt2:
                    chrg_peak_time_rate.append(adar_algorithm.dianjia(a1.iloc[0][-1], a1.iloc[-1][-1])[0])
                    chrg_noun_time_rate.append(adar_algorithm.dianjia(a1.iloc[0][-1], a1.iloc[-1][-1])[1])
                    chrg_vally_time_rate.append(adar_algorithm.dianjia(a1.iloc[0][-1], a1.iloc[-1][-1])[2])
                    chrg_time_score.append(adar_algorithm.dianjia(a1.iloc[0][-1], a1.iloc[-1][-1])[3])
                else:
                    chrg_peak_time_rate.append(adar_algorithm.dianjia(a1.iloc[0][-1], timeStamp)[0])
                    chrg_peak_time_rate.append(adar_algorithm.dianjia(timeStamp, a1.iloc[-1][-1])[0])
                    chrg_noun_time_rate.append(adar_algorithm.dianjia(a1.iloc[0][-1], timeStamp)[1])
                    chrg_noun_time_rate.append(adar_algorithm.dianjia(timeStamp, a1.iloc[-1][-1])[1])
                    chrg_vally_time_rate.append(adar_algorithm.dianjia(a1.iloc[0][-1], timeStamp)[2])
                    chrg_vally_time_rate.append(adar_algorithm.dianjia(timeStamp, a1.iloc[-1][-1])[2])
                    chrg_time_score.append(adar_algorithm.dianjia(a1.iloc[0][-1], timeStamp)[3])
                    chrg_time_score.append(adar_algorithm.dianjia(timeStamp, a1.iloc[-1][-1])[3])
            chrg_peak_time_rate = round(sum(chrg_peak_time_rate) / len(r), 2)
            chrg_noun_time_rate = round(sum(chrg_noun_time_rate) / len(r), 2)
            chrg_vally_time_rate = round(sum(chrg_vally_time_rate) / len(r), 2)
            chrg_time_score = round(sum(chrg_time_score) / len(r), 2)
            chrg_score = (
                         chrg_dc_fery_score + dsch_over_fery_score + dsch_dod_score + chrg_end_fery_score + chrg_time_score) / 5
            shijianduan_key = "chrg_peak_time_rate,chrg_noun_time_rate,chrg_vally_time_rate,chrg_time_score,chrg_score"
            shijianduan_value = (
                round(chrg_peak_time_rate, 2), round(chrg_noun_time_rate, 2), round(chrg_vally_time_rate, 2),
                round(chrg_time_score, 2), round(chrg_score, 2))
            print('许志宇负责')

            # 温度一致性（刘东负责）
            temp_con = Temp_con(conn)
            num = temp_con.temp_num(car_vh, start_time, end_time)
            if len(num) < 10:
                for i in range(0, 10 - len(num)):
                    num.append(0)
            frey = temp_con.temp_frey(car_vh, start_time, end_time)
            if len(frey) < 10:
                for i in range(0, 10 - len(frey)):
                    frey.append(0)
            unifomity = temp_con.temp_unifomity(car_vh, start_time, end_time)
            temp_con_key = 'temp_max_no1,temp_max_no2,temp_max_no3,temp_max_no4,temp_max_no5,temp_max_no6,temp_max_no7,temp_max_no8,temp_max_no9,temp_max_no10, temp_max_frey_no1, temp_max_frey_no2, temp_max_frey_no3, temp_max_frey_no4, temp_max_frey_no5, temp_max_frey_no6, temp_max_frey_no7, temp_max_frey_no8, temp_max_frey_no9, temp_max_frey_no10 ,temp_consiy_eval'
            temp_con_value = (
                num[0], num[1], num[2], num[3], num[4], num[5], num[6], num[7], num[8], num[9], frey[0],
                round(frey[1], 2),
                round(frey[2], 2), round(frey[3], 2), round(frey[4], 2), round(frey[5], 2), round(frey[6], 2),
                round(frey[7], 2), round(frey[8], 2), round(frey[9], 2), unifomity)
            print('刘东负责')

            # 王一全（电压一致性）
            volt_unifomity = Volt_unifomity(conn, car_vh, start_time, end_time)
            volt_unit_grade = volt_unifomity.battery_grades()
            volt_unit_key = volt_unifomity.battery_keys()
            cell_volt_max_no_key = 'cell_volt_max_no1, cell_volt_max_no2,cell_volt_max_no3, cell_volt_max_no4,  cell_volt_max_no5, cell_volt_max_no6, cell_volt_max_no7, cell_volt_max_no8,cell_volt_max_no9,cell_volt_max_no10'
            module_volt_max_no_key = 'module_volt_max_no1, module_volt_max_no2, module_volt_max_no3, module_volt_max_no4, module_volt_max_no5, module_volt_max_no6, module_volt_max_no7, module_volt_max_no8, module_volt_max_no9, module_volt_max_no10'
            cell_volt_max_no_value = (
                volt_unit_key[1][0], volt_unit_key[1][1], volt_unit_key[1][2], volt_unit_key[1][3], volt_unit_key[1][4],
                volt_unit_key[1][5], volt_unit_key[1][6], volt_unit_key[1][7], volt_unit_key[1][8], volt_unit_key[1][9])
            module_volt_max_no_value = (
                volt_unit_key[0][0], volt_unit_key[0][1], volt_unit_key[0][2], volt_unit_key[0][3], volt_unit_key[0][4],
                volt_unit_key[0][5], volt_unit_key[0][6], volt_unit_key[0][7], volt_unit_key[0][8], volt_unit_key[0][9])
            # print (volt_unit_grade[-1])
            volt_consiy_eval_thistime_value = (volt_unit_grade[-1],)
            volt_consiy_eval_thistime_key = 'volt_consiy_eval'
            volt_max_frey_no_key = 'volt_max_frey_no1, volt_max_frey_no2, volt_max_frey_no3, volt_max_frey_no4, volt_max_frey_no5, volt_max_frey_no6,volt_max_frey_no7,volt_max_frey_no8,volt_max_frey_no9,volt_max_frey_no10'
            volt_max_frey_no_value = (
                round(volt_unit_grade[0], 2), round(volt_unit_grade[1], 2), round(volt_unit_grade[2], 2),
                round(volt_unit_grade[3], 2), round(volt_unit_grade[4], 2), round(volt_unit_grade[5], 2),
                round(volt_unit_grade[6], 2), round(volt_unit_grade[7], 2), round(volt_unit_grade[8], 2),
                round(volt_unit_grade[9], 2))
            volt_unifomity_value = module_volt_max_no_value + cell_volt_max_no_value + volt_max_frey_no_value + volt_consiy_eval_thistime_value
            volt_unifomity_key = module_volt_max_no_key + ',' + cell_volt_max_no_key + ',' + volt_max_frey_no_key + ',' + volt_consiy_eval_thistime_key
            print('王一全负责')

            # 电量计算（马非凡负责）
            bat_sys_cap_change = Bat_sys_cap_change(conn)
            capacity_result = bat_sys_cap_change.cal_Bat_sys_cap_change(start_time, end_time, car_vh)
            capacity_bat = capacity_result[0]  # 总的电池容量
            chrg_cap_total = capacity_result[1]  # 总的电表读数
            bat_sys_cap_change_key = 'capacity_bat,chrg_cap_total'
            bat_sys_cap_change_value = (round(capacity_bat, 2), round(chrg_cap_total, 2))
            print('马非凡电量计算')

            # 数据库写入
            company_short_name = Search_company_short_name(car_vh, conn)
            print(company_short_name)
            add_time = int(time.time())
            week = Week(add_time)
            add_yearmon = add_year_mon(add_time)
            add_day = Add_day(add_time)
             necessary_key = 'company_short_name,car_vh,add_time,week,add_yearmon,add_day'
            necessary_value = (company_short_name, car_vh, add_time, week, add_yearmon, add_day)
            total_key = necessary_key + ',' + adar_algorithm_key + ',' + temp_con_key + ',' + volt_unifomity_key + ',' + bat_sys_cap_change_key + ',' + shijianduan_key
            total_value = necessary_value + adar_algorithm_value + temp_con_value + volt_unifomity_value + bat_sys_cap_change_value + shijianduan_value
            sql = "INSERT INTO diagnosis.t_car_week_results (%s) VALUES %s;" % (total_key, total_value)
            print(sql)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            # except Exception as e:
            #     print("出现问题" + str(e))
            # finally:

            conn.close()
            return 'ok'
        except Exception as e:
            print("出现问题" + str(e))
            return str(e)
        # finally:
        #     conn.commit()
        #     cursor.close()
        #     conn.close()
        # return "ok"


    else:
        return "415 Unsupported Media Type ;)"


if __name__ == '__main__':
    app.run(debug=True)
    # app.run(host='0.0.0.0', port=80, debug=True)
