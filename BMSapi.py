from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from fastapi.responses import JSONResponse
import pytz
from datetime import datetime
import json

app = FastAPI()

origins = [
    "*",
    # Add more origins as needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

file_path = "/home/tenet/ems/py_script/db_creds.json"

with open(file_path, 'r') as file:
    data = json.load(file)

def get_emsdb():
    db = mysql.connector.connect(
        host=data['awsDB']['host'],
        user=data['awsDB']['user'],
        password=data['awsDB']['password'],
        database='EMS',
        port=data['awsDB']['port']
    )
    return db

def get_bmsdb():
    db=mysql.connector.connect(
        host=data['bmsDB']['host'],
        user=data['bmsDB']['user'],
        password=data['bmsDB']['password'],
        port=data['bmsDB']['port']
    )
    return db


def custom_round(number):
    last_two_digits = number % 100
    if last_two_digits < 50:
        return (number // 100) * 100
    else:
        return ((number // 100) + 1) * 100



@app.post('/PeakDemand/Analysis/Count/Peak/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_bmsdb)):
    peaklis = []

    try:
        value = data.get('date')
        peak = data.get('peak')

        if value and peak and isinstance(value, str):
            with db.cursor() as bms_cur:
                awsdb = get_emsdb()
                awscur = awsdb.cursor()

                awscur.execute("SELECT maxAvgPeak FROM EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 1;")

                peakMax = awscur.fetchall()

                maxAvg = peakMax[0][0]

                maxAvg = round(maxAvg)

                GdmaxAvg = maxAvg - ((maxAvg*5)/100)

                peak = round(peak)

                bms_cur.execute(f"SELECT count(totalApparentPower2) FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = '{value}' and totalApparentPower2 >= {peak};")

                maxres = bms_cur.fetchall()

                maxCount = maxres[0][0]

                bms_cur.execute(f"SELECT count(totalApparentPower2) FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = '{value}' and totalApparentPower2 < {peak};")

                minres = bms_cur.fetchall()

                minCount = minres[0][0]

                totalCount = minCount+maxCount

                maxPr = (maxCount/totalCount)*100
                minPr = (minCount/totalCount)*100

                peaklis.append({'limit':peak,'maxCount':maxCount,'minCount':minCount,'maxpr':maxPr,'minpr':minPr,
                                'thresholdlimit':maxAvg,'graduallimit':GdmaxAvg})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return peaklis


@app.get('/PeakDemand/Analysis/Count')
def peak_demand_date(db: mysql.connector.connect = Depends(get_bmsdb)):

    peaklis=[]
    try:
        processed_db = get_bmsdb()
        awsdb = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    

    awscur = awsdb.cursor()
    bms_cur = processed_db.cursor()

    awscur.execute("SELECT maxAvgPeak FROM EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 1;")

    peakMax = awscur.fetchall()

    maxAvg = peakMax[0][0]

    maxAvg = round(maxAvg)

    GdmaxAvg = maxAvg - ((maxAvg*5)/100)

    bms_cur.execute(f"SELECT count(totalApparentPower2) FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = curdate() and totalApparentPower2 >= {maxAvg};")

    maxres = bms_cur.fetchall()

    maxCount = maxres[0][0]

    bms_cur.execute(f"SELECT count(totalApparentPower2) FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = curdate() and totalApparentPower2 < {maxAvg};")

    minres = bms_cur.fetchall()

    minCount = minres[0][0]

    totalCount = minCount+maxCount

    maxPr = (maxCount/totalCount)*100
    minPr = (minCount/totalCount)*100

    peaklis.append({'limit':GdmaxAvg,'maxCount':maxCount,'minCount':minCount,'maxpr':maxPr,'minpr':minPr,'thresholdlimit':maxAvg,
                    'GradualLimit':GdmaxAvg})

    return peaklis


@app.post('/PeakDemand/Analysis/Count/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_bmsdb)):
    peaklis = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:
                try:
                    awsdb = get_emsdb()
                except Exception as e:
                    raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
                
                awscur = awsdb.cursor()

                awscur.execute(f"SELECT maxAvgPeak FROM EMS.peakShavingLogic where date(polledTime) = '{value}' order by polledTime desc limit 1;")

                peakMax = awscur.fetchall()

                maxAvg = peakMax[0][0]

                maxAvg = round(maxAvg)

                GdmaxAvg = maxAvg - ((maxAvg*5)/100)

                bms_cur.execute(f"SELECT count(totalApparentPower2) FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = '{value}' and totalApparentPower2 >= {maxAvg};")

                maxres = bms_cur.fetchall()

                maxCount = maxres[0][0]

                bms_cur.execute(f"SELECT count(totalApparentPower2) FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = '{value}' and totalApparentPower2 < {maxAvg};")

                minres = bms_cur.fetchall()

                minCount = minres[0][0]

                totalCount = minCount+maxCount

                maxPr = (maxCount/totalCount)*100
                minPr = (minCount/totalCount)*100

                peaklis.append({'limit':maxAvg,'maxCount':maxCount,'minCount':minCount,'maxpr':maxPr,'minpr':minPr,
                                'thresholdlimit':maxAvg,'GradualLimit':GdmaxAvg})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return peaklis


@app.get('/PeakDemand/Maximum')
def peak_demand_date(db: mysql.connector.connect = Depends(get_bmsdb)):

    MaximumDemand=[]
    try:
        processed_db = get_bmsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT totalApparentPower2,polledTime FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) =curdate() and totalApparentPower2 = (select max(totalApparentPower2) from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) =curdate())")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[1])[11:19]
        if(i[0]==None):
            totalApparentPower2=0
        else:
            totalApparentPower2=round(i[0],2)
        MaximumDemand.append({'totalApparentPower2':totalApparentPower2,"PolledTime":polledTime})
    
    return MaximumDemand

@app.post('/PeakDemand/Maximum/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_bmsdb)):
    MaximumDemand = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:

                bms_cur.execute(f"SELECT totalApparentPower2,polledTime FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = '{value}' and totalApparentPower2 = (select max(totalApparentPower2) from bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = '{value}')")
   
                res = bms_cur.fetchall()

                # print(res)

                for i in res:
                    polledTime = str(i[1])[11:19]
                    if(i[0]==None):
                        totalApparentPower2=0
                    else:
                        totalApparentPower2=round(i[0],2)
                    
                    MaximumDemand.append({'totalApparentPower2':totalApparentPower2,"PolledTime":polledTime})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return MaximumDemand

@app.get('/thermal/dashbaordSummary')
def peak_demand_min():
    thermal_list = []
    try:
        processed_db = get_bmsdb()
        awsdb = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})

    bms_cur = processed_db.cursor()
    awscur = awsdb.cursor()

    bms_cur.execute("SELECT thermalStorageInlet/100,thermalStorageOutlet/100,tsStoredWaterTemperature/100,ADPValve,BDPValve,HValve  FROM bmsmgmtprodv13.thermalStorageMQTTReadings where date(polledTime) = curdate() and ADPValve>0 order by recordId desc limit 1;")

    thermalres= bms_cur.fetchall()

    if len(thermalres) > 0:
        tsIn = thermalres[0][0]
        tsOut = thermalres[0][1]
        tsSt = thermalres[0][2]
        ADPvalve = thermalres[0][3]
        BDPvalve = thermalres[0][4]
        Hvalve = thermalres[0][5]

        if ADPvalve == 1 and BDPvalve ==1 and Hvalve == 1:
            status = 'DCHG'
        else:
            status = 'IDLE'
    else:
        tsIn = None
        tsOut = None
        tsSt = None
        status = None

    awscur.execute("SELECT sum(coolingEnergy),sum(ChargingEnergy) FROM EMS.ThermalHourly where date(polledTime) = curdate();")

    energyRes = awscur.fetchall()

    if len(energyRes) > 0:
        coolingEnergy = energyRes[0][0]
        chargingEnergy = energyRes[0][1]
    else:
        coolingEnergy = None
        chargingEnergy = None

    thermal_list.append({'tsInletTemperature':tsIn,'tsOutletTemperature':tsOut,'tsStoredWaterTemperature':tsSt,
                         'coolingEnergy':coolingEnergy,'chargingEnergy':chargingEnergy,'Status':status})
    
    return thermal_list
    

@app.get('/thermal/summaryCard')
def peak_demand_min():
    thermal_list = []
    try:
        processed_db = get_bmsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})

    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT tsStoredWaterTemperature,thermalStorageInlet,thermalStorageOutlet,linePressHigh,dischargingFlowrate,ADPValve,BDPValve,HValve,actuatorPosition,actualChargingFlowrate,polledTime  FROM bmsmgmtprodv13.thermalStorageMQTTReadings where date(polledTime) = curdate() and ADPValve>0 order by recordId desc limit 1;")

    thermalres= bms_cur.fetchall()

    try:
        tsstored = thermalres[0][0]/100
    except:
        tsstored = None
    try:
        tsInlet = thermalres[0][1]/100
    except:
        tsInlet = None
    try:
        tsOultlet = thermalres[0][2]/100
    except:
        tsOultlet = None
    try:
        linePress = thermalres[0][3]/100
    except:
        linePress = None
    try:
        flowrate = thermalres[0][4]
    except:
        flowrate = None
    try:
        flowrateTS = round(thermalres[0][9]/100,3)
    except:
        flowrateTS = None
    try:
        Adpvalve = thermalres[0][5]
    except:
        Adpvalve = None
    try:
        Bdpvalve = thermalres[0][6]
    except:
        Bdpvalve = None
    try:
        Hvalve = thermalres[0][7]
    except:
        Hvalve = None
    try:
        actuator = thermalres[0][8]
    except:
        actuator = None

    bms_cur.execute("SELECT chargingPump1Power,chargingPump2Power FROM  bmsmgmt_olap_prod_v13.hvacChillerElectricPolling where date(polledTime) = curdate() order by polledTime desc limit 1;")

    statusres = bms_cur.fetchall()

    try:
        chargingpump1 = statusres[0][0]
    except:
        chargingpump1 = None
    try:
        chargingpump2 = statusres[0][1]
    except:
        chargingpump2 = None


    thermal_list.append({"storedwatertemperature":tsstored,"inletTemparature":tsInlet,"outletTemparature":tsOultlet,
                         "thermalStoragelinepressure":linePress,"flowrateToBuilding":flowrate,"flowrateToTS":flowrateTS,
                         "ADPvalveStatus":Adpvalve,"BDPvalveStatus":Bdpvalve,"HvalveStatus":Hvalve,"ActuvatorStatus":actuator,
                         "chargingPump1Power":chargingpump1,"chargingPump2Power":chargingpump2,"polledTime":thermalres[0][10]})
    
    bms_cur.close()
    processed_db.close()
    
    return thermal_list
    

@app.get('/peakDemandmin')
def peak_demand_min():
    peak_list = []
    try:
        processed_db = get_bmsdb()
        awsdb = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})

    bms_cur = processed_db.cursor()
    awscur = awsdb.cursor()

    awscur.execute("SELECT round(maxAvgPeak) FROM EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 1;")

    maxAvgres = awscur.fetchall()

    if len(maxAvgres) > 0:
        maxAvg = maxAvgres[0][0]
    else:
        maxAvg = 4000

    safeLimit = maxAvg - ((maxAvg*5)/100)

    bms_cur.execute("SELECT polledTime,totalApparentPower2 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling WHERE DATE(polledTime) = curdate()")

    peaks = bms_cur.fetchall()

    for i in peaks:
        if i[1] is not None:
            polled_time = str(i[0])[11:16]
            peak_list.append({'polledTime': polled_time, 'peakdemand': round(i[1], 2), 'limitline': maxAvg, 'safeLimit':safeLimit})
        else:
            polled_time = str(i[0])[11:16]
            peak_list.append({'polledTime': polled_time, 'peakdemand': None, 'limitline': maxAvg, 'safeLimit':safeLimit})

    bms_cur.close()
    processed_db.close()

    return peak_list

@app.post('/peakDemandDate')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_bmsdb)):
    peak_list = []

    try:
        value = data.get('date')

        try:
            awsdb = get_emsdb()
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:

                awscur = awsdb.cursor()

                awscur.execute(f"SELECT round(maxAvgPeak) FROM EMS.peakShavingLogic where date(polledTime) = '{value}' order by polledTime desc limit 1;")

                maxAvgres = awscur.fetchall()

                if len(maxAvgres) >0:
                    maxAvg = maxAvgres[0][0]
                else:
                    maxAvg = 4000
                
                safeLimit = maxAvg - ((maxAvg*1)/100)

                bms_cur.execute(f"SELECT polledTime,totalApparentPower2 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling WHERE DATE(polledTime) = '{value}'")

                peaks = bms_cur.fetchall()

                for i in peaks:
                    if i[1] is not None:
                        polled_time = str(i[0])[11:16]
                        peak_list.append({'polledTime': polled_time, 'peakdemand': round(i[1], 2), 'limitline': maxAvg, 'safeLimit':safeLimit})
                    else:
                        polled_time = str(i[0])[11:16]
                        peak_list.append({'polledTime': polled_time, 'peakdemand': None, 'limitline': maxAvg, 'safeLimit':safeLimit})


                bms_cur.close()

    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return peak_list


@app.get('/grid/initialgraph')
def peak_demand_min():
    grid_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})

    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledDate,Energy FROM EMS.GridProcessed where polledDate >= date_sub(curdate(), interval 6 day)  and polledDate <= curdate();")

    grids = bms_cur.fetchall()

    for i in grids:
        if i[1] is not None:
            polled_time = str(i[0])
            grid_list.append({'polledTime': polled_time, 'grid': round(i[1], 2)})

    bms_cur.close()
    processed_db.close()

    return grid_list


@app.post('/grid/filter')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    grid_list = []

    try:
        value1 = data.get('date')
        value2 = data.get('endDate')

        if value1 and value2 and isinstance(value1, str) and isinstance(value2, str):
            with db.cursor() as bmscur:

                bmscur.execute(
                    "SELECT polledDate,Energy FROM EMS.GridProcessed where polledDate >= %s  and polledDate <= %s;",(value1,value2)
                )

                grids = bmscur.fetchall()

                for i in grids:
                    if i[1] is not None:
                        polled_time = str(i[0])
                        grid_list.append({'polledTime': polled_time, 'grid': round(i[1], 2)})

        elif value1 and isinstance(value1, str):
            with db.cursor() as bmscur:

                bmscur.execute(
                    "SELECT polledDate,Energy FROM EMS.GridProcessed where polledDate >= %s  and polledDate <= curdate();",(value1,)
                )

                peaks = bmscur.fetchall()

                for i in peaks:
                    if i[1] is not None:
                        polled_time = str(i[0])
                        grid_list.append({'polledTime': polled_time, 'grid': round(i[1], 2)})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return grid_list


@app.get('/peak/initialgraph')
def peak_demand_min():
    peak_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})

    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledTime, peakdemand FROM EMS.peakdemanddaily where polledTime >= date_sub(curdate(), interval 6 day)  and polledTime <= curdate();")

    peaks = bms_cur.fetchall()

    for i in peaks:
        if i[1] is not None:
            polled_time = str(i[0])
            peak_list.append({'polledTime': polled_time, 'peakdemand': round(i[1], 2)})

    bms_cur.close()
    processed_db.close()

    return peak_list


@app.post('/peak/filter')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    peak_list = []

    try:
        value1 = data.get('date')
        value2 = data.get('endDate')

        if value1 and value2 and isinstance(value1, str) and isinstance(value2, str):
            with db.cursor() as bmscur:

                bmscur.execute(
                    "SELECT polledTime,peakdemand FROM EMS.peakdemanddaily where polledTime >= %s and polledTime <= %s;",(value1,value2)
                )

                peaks = bmscur.fetchall()

                for i in peaks:
                    if i[1] is not None:
                        polled_time = str(i[0])
                        peak_list.append({'polledTime': polled_time, 'peakdemand': round(i[1], 2)})

        elif value1 and isinstance(value1, str):
            with db.cursor() as bmscur:

                bmscur.execute(
                    "SELECT polledTime,peakdemand FROM EMS.peakdemanddaily where polledTime >= %s and polledTime <= curdate();",(value1,)
                )

                peaks = bmscur.fetchall()

                for i in peaks:
                    if i[1] is not None:
                        polled_time = str(i[0])
                        peak_list.append({'polledTime': polled_time, 'peakdemand': round(i[1], 2)})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return peak_list


@app.get('/chillerstatus')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    chiller_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""SELECT chiller1,chiller2,chiller3,chiller4,chiller5,chiller6,chiller7,chiller8 
                    FROM EMS.ChillerStatus 
                    where date(polledTime) = curdate() 
                    order by polledTime desc limit 1;""")

    res = bms_cur.fetchall()

    for i in res:
        chiller_list.append({'chiller1':i[0],'chiller2':i[1],'chiller3':i[2],'chiller4':i[3],
                             'chiller5':i[4],'chiller6':i[5],'chiller7':i[6],'chiller8':i[7]})
        
    bms_cur.close()
    processed_db.close()
    
    return chiller_list

@app.get('/BuildingConsumptionPage2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    building_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledTime,gridEnergy,rooftopEnergy,wheeledinEnergy,thermalDischarge,peakDemand FROM EMS.buildingConsumption where date(polledTime) = curdate() order by polledTime;")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        thermalDischarge = i[4]
        if thermalDischarge == None:
            thermalDischarge = 0
        else:
            thermalDischarge = abs(thermalDischarge)
        building_list.append({'Timestamp':polledTime,'GridEnergy':i[1],'RooftopEnergy':i[2],
                              'WheeledInSolar':i[3],'thermalDischarge':thermalDischarge})
        
    bms_cur.close()
    processed_db.close()
    
    return building_list


@app.post('/filteredGraph/BuildingConsumptionPage2')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    building_list = []

    try:
        value = data.get('date')
        #print(value,type(value))

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                
                bmscur.execute(
                    f"SELECT polledTime,gridEnergy,rooftopEnergy,wheeledinEnergy,thermalDischarge FROM EMS.buildingConsumption where date(polledTime) = '{value}' order by polledTime;")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    thermalDischarge = i[4]
                    if thermalDischarge == None:
                        thermalDischarge = 0
                    else:
                        thermalDischarge = abs(thermalDischarge)
                    building_list.append({'Timestamp':polledTime,'GridEnergy':i[1],'RooftopEnergy':i[2],
                                        'WheeledInSolar':i[3],'thermalDischarge':thermalDischarge})
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)
    
    #print(building_list)
    return building_list

@app.get('/buildingConsumptionHighlights')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    building_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""SELECT date(polledTime),
                                  sum(gridEnergy),
                                  sum(rooftopEnergy),
                                  sum(wheeledinEnergy),
                                  sum(wheeledinEnergy2),
                                  sum(windEnergy),
                                  sum(diesel),
                                  max(peakDemand) 
                                  FROM EMS.buildingConsumption 
                                  where date(polledTime) = curdate();""")
    
    res = bms_cur.fetchall()

    for i in res:
        building_list.append({'gridEnergy':i[1],'rooftopEnergy':i[2],'wheeledinEnergy':i[3],'wheeledinEnergy2':i[4],
                              'windEnergy':i[5],'peakDemand':i[7],'Diesel':i[6]})
    
    bms_cur.close()
    processed_db.close()

    return building_list

@app.post('/filtered/buildingConsumptionHighlights')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    building_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"""SELECT date(polledTime),sum(gridEnergy),sum(rooftopEnergy),sum(wheeledinEnergy),sum(wheeledinEnergy2),
                                 sum(windEnergy),sum(diesel),max(peakDemand) FROM EMS.buildingConsumption where date(polledTime) = '{value}';""")

                res = bmscur.fetchall()

                for i in res:
                    building_list.append({'gridEnergy':i[1],'rooftopEnergy':i[2],'wheeledinEnergy':i[3],'wheeledinEnergy2':i[4],
                              'windEnergy':i[5],'peakDemand':i[7],'Diesel':i[6]})
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return building_list

@app.get('/Ltoanalytics/energy_VS_packsoc')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select polledTime,Energy,packSoc,batterySts from EMS.LTOMinWise where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        lto_list.append({'packsoc':i[2],'batteryEnergy':i[1],'timestamp':polledTime,'batteryStatus':i[3]})

    bms_cur.close()
    processed_db.close()

    return lto_list    
    

@app.post('/filtered/Ltoanalytics/energy_VS_packsoc')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"select polledTime,Energy,packSoc,batterySts from EMS.LTOMinWise where date(polledTime) = '{value}';")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[1])[11:16]
                    polledTime = str(i[0])[11:16]
                    lto_list.append({'packsoc':i[2],'batteryEnergy':i[1],'timestamp':polledTime,'batteryStatus':i[3]})
                    

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return lto_list

@app.get('/current_VS_voltage/LTOBattery')
def peak_demand_date(db: mysql.connector.connect = Depends(get_bmsdb)):
    lto_list = []
    try:
        processed_db = get_bmsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""SELECT recordTimestamp,batteryVoltage,batteryCurrent,
                    hour(recordTimestamp) as hr,minute(recordTimestamp) as mint 
                    FROM bmsmgmtprodv13.ltoBatteryData 
                    where date(recordTimestamp) = curdate() group by hr,mint;""")
    
    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        lto_list.append({'polledTime':polledTime,'BatteryVoltage':i[1],'BatteryCurrent':i[2]})

    bms_cur.close()
    processed_db.close()

    return lto_list


@app.post('/filtered/current_VS_voltage/LTOBattery')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_bmsdb)):
    lto_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT recordTimestamp,batteryVoltage,batteryCurrent,hour(recordTimestamp) as hr,minute(recordTimestamp) as mint FROM bmsmgmtprodv13.ltoBatteryData where date(recordTimestamp) = '{value}' group by hr,mint;")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    lto_list.append({'polledTime':polledTime,'BatteryVoltage':i[1],'BatteryCurrent':i[2]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return lto_list


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5001) #,ssl_keyfile="key.pem", ssl_certfile="cert.pem")