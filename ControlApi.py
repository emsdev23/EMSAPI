from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from fastapi.responses import JSONResponse
import psycopg2
import json

app = FastAPI()

origins = [
    "*",
    # Add more origins as needed
]

file_path = "/home/tenet/ems/py_script/db_creds.json"

with open(file_path, 'r') as file:
    data = json.load(file)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_emsdb():
    db = mysql.connector.connect(
        host=data['emsDB']['host'],
        user=data['emsDB']['user'],
        password=data['emsDB']['password'],
        database='EMS',
        port=data['emsDB']['port']
    )
    return db

def get_awsdb():
    db = mysql.connector.connect(
        host=data['awsDB']['host'],
        user=data['awsDB']['user'],
        password=data['awsDB']['password'],
        database='EMS',
        port=data['awsDB']['port']
    )
    return db

def get_cpm_2():
    db = psycopg2.connect(
            host=data["cmp2"]["host"],
            database=data["cmp2"]["database"],
            user=data["cmp2"]["user"],
            password=data["cmp2"]["password"]
            )
    
    return db

API_mapping = {
    '1': 'http://localhost:8009/ioesinglestr',
    '2': 'http://localhost:8009/ioedoublestr',
    '3': 'http://localhost:8009/ioetriplestr',
    '4': 'http://localhost:8009/ioefourstr',
    '5': 'http://localhost:8009/ioefivestr',
}


@app.get('/ExcessRE/Details')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):

    ExcessREData=[]
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    ems_cur = processed_db.cursor()

    ems_cur.execute("SELECT polledTime,ExcessRE,Duration,Stored_in,DeficitRE FROM EMS.ExcessREcard where date(polledTime)=curdate()")
   
    res = ems_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[0])[11:19]
        if(i[1]==None):
            ExcessRE=0
        else:
            ExcessRE=round(i[1],2)
        if(i[2]==None):
            Duration=0
        else:
            Duration=round(i[2],2)

        if(i[3]==None):
            Stored_in=0
        else:
            Stored_in=round(i[3],2)

        if(i[4]==None):
            DeficitRE=0
        else:
            DeficitRE=(round(i[4],2)*-1)

        ExcessREData.append({'polledTime':polledTime,"ExcessRE":ExcessRE,"Duration":Duration,"Stored_in":Stored_in,"DeficitRE":DeficitRE})
    
    return ExcessREData


@app.post('/control/hourlyDetails/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    hourlyLi = []

    try:
        value = data.get('date')

        if value:
            with db.cursor() as awscur:

                awscur.execute(f"""select polledTime,gridEnergy,rooftopEnergy,wheeledinEnergy,wheeledinEnergy2,windEnergy,diesel,peakDemand 
                   from EMS.buildingConsumption where date(polledTime) = '{value}';""")
    
                res = awscur.fetchall()
                hourlyLi = []

                if len(res) > 0:
                    for i in res:
                        if i[0] != None:
                            polledTime = str(i[0])[11:16]
                        
                        if i[1] != None:
                            grid = round(i[1],1)
                        else:
                            grid = 0
                        
                        if i[2] != None:
                            roof = round(i[2],1)
                        else:
                            roof = 0

                        if i[3] != None:
                            wheel = round(i[3],1)
                        else:
                            wheel = 0
                        
                        if i[4] != None:
                            wheel2 = round(i[4],1)
                        else:
                            wheel2 = 0
                        
                        if i[5] != None:
                            wind = round(i[5],1)
                        else:
                            wind = 0
                        
                        if i[6] != None:
                            diesel = round(i[6],1)
                        else:
                            diesel = 0

                        if i[7] != None:
                            peak = round(i[7],1)
                        else:
                            peak = 0

                        excess = round((wheel+wheel2+wind+roof) - (grid+diesel),1)

                        totalEnergy = round(grid+roof+diesel,1)
                        
                        if grid > (wheel+wheel2+wind):
                            grid = (grid+diesel) - (wheel+wheel2+wind)
                        else:
                            grid = 0

                        if excess < 0:
                            excess = 0

                        totalGrid = round(grid + diesel,1)
                        totalRE = round(wheel+wheel2+wind+roof,1)

                        hourlyLi.append({'polledTime':polledTime,'power':peak,'grid':totalGrid,'RE':totalRE,'excessRE':excess,'totalEnergy':totalEnergy})

                awscur.execute(f"SELECT polledTime,discharhingEnergy FROM EMS.UPSbatteryHourly where date(polledTime) = '{value}';")

                upsRes = awscur.fetchall()

                upsLi = []

                for i in upsRes:
                    polledTime = str(i[0])[11:16]
                    if i[1]!= None:
                        ups = round(i[1],1)
                    else:
                        ups = 0
                    upsLi.append({"polledTime":polledTime,"UPS":abs(ups)})

                for entry in hourlyLi:
                    for battery in upsLi:
                        if entry['polledTime'] == battery['polledTime']:
                            entry['UPS'] = battery['UPS']

                awscur.execute(f"SELECT polledTime,dischargingEnergy FROM EMS.LTObatteryHourly where date(polledTime) = '{value}';")

                ltoRes = awscur.fetchall()

                ltoLi = []

                for i in ltoRes:
                    polledTime = str(i[0])[11:16]
                    if i[1] != None:
                        lto = round(i[1],1)
                    else:
                        lto = 0
                    ltoLi.append({'polledTime':polledTime,'lto':abs(lto)})

                for entry in hourlyLi:
                    for battery in ltoLi:
                        if entry['polledTime'] == battery['polledTime']:
                            entry['lto'] = battery['lto']  
                
                awscur.execute(f"""SELECT polledTime,st1dischargingEnergy,st2dischargingEnergy,st3dischargingEnergy,st4dischargingEnergy,st5dischargingEnergy
                        FROM EMS.IOEbatteryHourly where date(polledTime) = '{value}';""")
                
                ioeRes = awscur.fetchall()
                ioeLi = []

                for i in ioeRes:
                    polledTime = str(i[0])[11:16]
                    if i[1] != None:
                        str1 = i[1]
                    else:
                        str1 = 0
                    if i[2] != None:
                        str2 = i[2]
                    else:
                        str2 = 0
                    if i[3] != None:
                        str3 = i[3]
                    else:
                        str3 = 0
                    if i[4] != None:
                        str4 = i[4]
                    else:
                        str4 = 0
                    if i[5] != None:
                        str5 = i[5]
                    else:
                        str5 = 0
                    
                    ioeStr = str1+str2+str3+str4+str5
                    ioeLi.append({'polledTime':polledTime,'ioe':abs(ioeStr)})
                
                for entry in hourlyLi:
                    for battery in ioeLi:
                        if entry['polledTime'] == battery['polledTime']:
                            entry['ioe'] = battery['ioe']  

                for entry in hourlyLi:
                    try:
                        ioe = entry['ioe']
                        del entry['ioe']
                    except Exception as ex:
                        ioe = 0

                    try:
                        lto = entry['lto']
                        del entry['lto']
                    except Exception as ex:
                        lto = 0

                    try:
                        ups = entry['ups']
                        del entry['ups']
                    except Exception as ex:
                        ups = 0

                    entry['battery'] = round(ioe+lto+ups,1)

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return hourlyLi

@app.get('/control/hourlyDetails')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    hourlyLi = []
    try:
        awsdb = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    awscur = awsdb.cursor()

    awscur.execute("""select polledTime,gridEnergy,rooftopEnergy,wheeledinEnergy,wheeledinEnergy2,windEnergy,diesel,peakDemand 
                   from EMS.buildingConsumption where date(polledTime) = curdate();""")
    
    res = awscur.fetchall()
    hourlyLi = []

    if len(res) > 0:
        for i in res:
            if i[0] != None:
                polledTime = str(i[0])[11:16]
            
            if i[1] != None:
                grid = round(i[1],1)
            else:
                grid = 0
            
            if i[2] != None:
                roof = round(i[2],1)
            else:
                roof = 0

            if i[3] != None:
                wheel = round(i[3],1)
            else:
                wheel = 0
            
            if i[4] != None:
                wheel2 = round(i[4],1)
            else:
                wheel2 = 0
            
            if i[5] != None:
                wind = round(i[5],1)
            else:
                wind = 0
            
            if i[6] != None:
                diesel = round(i[6],1)
            else:
                diesel = 0

            if i[7] != None:
                peak = round(i[7],1)
            else:
                peak = 0

            excess = round((wheel+wheel2+wind+roof) - (grid+diesel),1)

            totalEnergy = round(grid+roof+diesel,1)
            
            if grid > (wheel+wheel2+wind):
                grid = (grid+diesel) - (wheel+wheel2+wind)
            else:
                grid = 0

            if excess < 0:
                excess = 0

            totalGrid = round(grid + diesel,1)
            totalRE = round(wheel+wheel2+wind+roof,1)

            hourlyLi.append({'polledTime':polledTime,'power':peak,'grid':totalGrid,'RE':totalRE,'excessRE':excess,'totalEnergy':totalEnergy})

    awscur.execute("SELECT polledTime,discharhingEnergy FROM EMS.UPSbatteryHourly where date(polledTime) = curdate();")

    upsRes = awscur.fetchall()

    upsLi = []

    for i in upsRes:
        polledTime = str(i[0])[11:16]
        if i[1]!= None:
            ups = round(i[1],1)
        else:
            ups = 0
        upsLi.append({"polledTime":polledTime,"UPS":abs(ups)})

    for entry in hourlyLi:
        for battery in upsLi:
            if entry['polledTime'] == battery['polledTime']:
                entry['UPS'] = battery['UPS']

    awscur.execute("SELECT polledTime,dischargingEnergy FROM EMS.LTObatteryHourly where date(polledTime) = curdate();")

    ltoRes = awscur.fetchall()

    ltoLi = []

    for i in ltoRes:
        polledTime = str(i[0])[11:16]
        if i[1] != None:
            lto = round(i[1],1)
        else:
            lto = 0
        ltoLi.append({'polledTime':polledTime,'lto':abs(lto)})

    for entry in hourlyLi:
        for battery in ltoLi:
            if entry['polledTime'] == battery['polledTime']:
                entry['lto'] = battery['lto']  
    
    awscur.execute("""SELECT polledTime,st1dischargingEnergy,st2dischargingEnergy,st3dischargingEnergy,st4dischargingEnergy,st5dischargingEnergy
            FROM EMS.IOEbatteryHourly where date(polledTime) = curdate();""")
    
    ioeRes = awscur.fetchall()
    ioeLi = []

    for i in ioeRes:
        polledTime = str(i[0])[11:16]
        if i[1] != None:
            str1 = i[1]
        else:
            str1 = 0
        if i[2] != None:
            str2 = i[2]
        else:
            str2 = 0
        if i[3] != None:
            str3 = i[3]
        else:
            str3 = 0
        if i[4] != None:
            str4 = i[4]
        else:
            str4 = 0
        if i[5] != None:
            str5 = i[5]
        else:
            str5 = 0
        
        ioeStr = str1+str2+str3+str4+str5
        ioeLi.append({'polledTime':polledTime,'ioe':abs(ioeStr)})
    
    for entry in hourlyLi:
        for battery in ioeLi:
            if entry['polledTime'] == battery['polledTime']:
                entry['ioe'] = battery['ioe']  

    for entry in hourlyLi:
        try:
            ioe = entry['ioe']
            del entry['ioe']
        except Exception as ex:
            ioe = 0

        try:
            lto = entry['lto']
            del entry['lto']
        except Exception as ex:
            lto = 0

        try:
            ups = entry['ups']
            del entry['ups']
        except Exception as ex:
            ups = 0

        entry['battery'] = round(ioe+lto+ups,1)

    return hourlyLi

@app.get('/control/UpsDetails')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    Ups_list = []
    
    try:
        emsdb = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    emscur = emsdb.cursor()

    emscur.execute("SELECT pack_usable_soc,upsbatterystatus,batteryvoltage,batterycurrent,contactorstatus,precontactorstatus FROM EMS.EMSUPSBattery where date(received_time) = curdate() order by upsrecordid desc limit 1;")

    res = emscur.fetchall()

    for i in res:
        Ups_list.append({'packSOC':i[0],'batteryStatus':i[1],'batteryVoltage':i[2],'batteryCurrent':i[3],
                         'mainConStatus':i[4],'preConStatus':i[5]})
    
    return Ups_list

@app.get("/control/ioeDetails")
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    Ioe_list = []
    Ioe_dict = {}

    try:
        emsdb = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    emscur = emsdb.cursor()

    emscur.execute("""SELECT batteryCurrent,batteryStatus,batteryVoltage,mainContactorStatus,prechargeContactorStatus,packSOC 
                FROM EMS.ioeSt1BatteryData where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
    
    res1 = emscur.fetchall()

    if len(res1) > 0:
        print(res1)
        Ioe_dict["batteryCurrent1"] = res1[0][0]
        Ioe_dict["batteryStatus1"] = res1[0][1]
        Ioe_dict["batteryVoltage1"] = res1[0][2]
        Ioe_dict["mainCon1"] = res1[0][3]
        Ioe_dict["preCon1"] = res1[0][4]
        Ioe_dict["packSoc1"] = res1[0][5]

    else:
        Ioe_dict["batteryCurrent1"] = None
        Ioe_dict["batteryStatus1"] = None
        Ioe_dict["batteryVoltage1"] = None
        Ioe_dict["mainCon1"] = None
        Ioe_dict["preCon1"] = None
        Ioe_dict["packSoc1"] = None
    
    emscur.execute("""SELECT batteryCurrent,batteryStatus,batteryVoltage,mainContactorStatus,prechargeContactorStatus,packSOC 
                FROM EMS.ioeSt2BatteryData where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
    
    res2 = emscur.fetchall()

    if len(res2) > 0:
        Ioe_dict["batteryCurrent2"] = res2[0][0]
        Ioe_dict["batteryStatus2"] = res2[0][1]
        Ioe_dict["batteryVoltage2"] = res2[0][2]
        Ioe_dict["mainCon2"] = res2[0][3]
        Ioe_dict["preCon2"] = res2[0][4]
        Ioe_dict["packSoc2"] = res2[0][5]
    else:
        Ioe_dict["batteryCurrent2"] = None
        Ioe_dict["batteryStatus2"] = None
        Ioe_dict["batteryVoltage2"] = None
        Ioe_dict["mainCon2"] = None
        Ioe_dict["preCon2"] = None
        Ioe_dict["packSoc2"] = None
    
    
    emscur.execute("""SELECT batteryCurrent,batteryStatus,batteryVoltage,mainContactorStatus,prechargeContactorStatus,packSOC 
                FROM EMS.ioeSt3BatteryData where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
    
    res3 = emscur.fetchall()

    if len(res3) > 0:
        Ioe_dict["batteryCurrent3"] = res3[0][0]
        Ioe_dict["batteryStatus3"] = res3[0][1]
        Ioe_dict["batteryVoltage3"] = res3[0][2]
        Ioe_dict["mainCon3"] = res3[0][3]
        Ioe_dict["preCon3"] = res3[0][4]
        Ioe_dict["packSoc3"] = res3[0][5]
    else:
        Ioe_dict["batteryCurrent3"] = None
        Ioe_dict["batteryStatus3"] = None
        Ioe_dict["batteryVoltage3"] = None
        Ioe_dict["mainCon3"] = None
        Ioe_dict["preCon3"] = None
        Ioe_dict["packSoc3"] = None
    
    emscur.execute("""SELECT batteryCurrent,batteryStatus,batteryVoltage,mainContactorStatus,prechargeContactorStatus,packSOC 
                FROM EMS.ioeSt4BatteryData where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
    
    res4 = emscur.fetchall()

    if len(res4) > 0:
        Ioe_dict["batteryCurrent4"] = res4[0][0]
        Ioe_dict["batteryStatus4"] = res4[0][1]
        Ioe_dict["batteryVoltage4"] = res4[0][2]
        Ioe_dict["mainCon4"] = res4[0][3]
        Ioe_dict["preCon4"] = res4[0][4]
        Ioe_dict["packSoc4"] = res4[0][5]
    else:
        Ioe_dict["batteryCurrent4"] = None
        Ioe_dict["batteryStatus4"] = None
        Ioe_dict["batteryVoltage4"] = None
        Ioe_dict["mainCon4"] = None
        Ioe_dict["preCon4"] = None
        Ioe_dict["packSoc4"] = None

    emscur.execute("""SELECT batteryCurrent,batteryStatus,batteryVoltage,mainContactorStatus,prechargeContactorStatus,packSOC 
                FROM EMS.ioeSt5BatteryData where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
    
    res5 = emscur.fetchall()

    if len(res5) > 0:
        Ioe_dict["batteryCurrent5"] = res5[0][0]
        Ioe_dict["batteryStatus5"] = res5[0][1]
        Ioe_dict["batteryVoltage5"] = res5[0][2]
        Ioe_dict["mainCon5"] = res5[0][3]
        Ioe_dict["preCon5"] = res5[0][4]
        Ioe_dict["packSoc5"] = res5[0][5]
    else:
        Ioe_dict["batteryCurrent5"] = None
        Ioe_dict["batteryStatus5"] = None
        Ioe_dict["batteryVoltage5"] = None
        Ioe_dict["mainCon5"] = None
        Ioe_dict["preCon5"] = None
        Ioe_dict["packSoc5"] = None

    emscur.close()
    
    Ioe_list.append(Ioe_dict)

    return Ioe_list

@app.post('/control/ioeControl')
def ioeControl(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    try:
        polledTime = data.get('polledTime')
        functionCode = data.get('functionCode')
        controlStatus = data.get('controlStatus')
        strings = data.get('strings')
        crate = data.get('crate')

        for i in strings:
            print(i)

        if functionCode == "OFF":
                ApiUrl = 'http://localhost:8009/ioeprocessoff'
                output_string = ""

                with db.cursor() as bms_cur:
                    sql = "INSERT INTO EMS.ioeInstantaneous(polledTime,functionCode,controlStatus,strings,httpLink,crate) values(%s,%s,%s,%s,%s,%s)"
                    val = (polledTime,functionCode,controlStatus,output_string,ApiUrl,crate)
                    
                    bms_cur.execute(sql,val)
                    db.commit()
                    return "Parameter Added Sucessfully"
        
        elif functionCode == "ON":
            if polledTime != None or functionCode != None or controlStatus !=None and strings != None:
                ApiUrl = API_mapping[str(len(strings))]
                output_string = ",".join(strings)
                ApiUrl = ApiUrl+'?strings='+output_string+','+controlStatus+'&crate='+crate
                print(ApiUrl)

            with db.cursor() as bms_cur:
                sql = "INSERT INTO EMS.ioeInstantaneous(polledTime,functionCode,controlStatus,strings,httpLink,crate) values(%s,%s,%s,%s,%s,%s)"
                val = (polledTime,functionCode,controlStatus,output_string,ApiUrl,crate)
                
                bms_cur.execute(sql,val)
                db.commit()
                return "Parameter Added Sucessfully"
            
        else:
            return "Parameters not available"
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)


@app.post('/control/HotwaterStorage')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):

    try:
        polledTime = data.get('polledTime')
        functionCode = data.get('functionCode')
        controlStatus = data.get('controlStatus')

        if polledTime != None or functionCode != None or controlStatus !=None :
            with db.cursor() as bms_cur:
                sql = "INSERT INTO EMS.hotWaterInstant(polledTime,functionCode,controlStatus) values(%s,%s,%s)"
                val = (polledTime,functionCode,controlStatus)
                
                bms_cur.execute(sql,val)
                db.commit()
                print("Hot Water Parameters added")
                return "Parameter Added Sucessfully"


    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)
    
@app.get('/control/HotwaterDetails')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    hotWater_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    try:
        awsdb = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    emscur = processed_db.cursor()
    awscur = awsdb.cursor()

    emscur.execute("SELECT storedWaterTemperature,tankFuildVolume,hotWaterSts FROM EMS.HotWaterStorage where date(recordtimestamp) = curdate() order by recordtimestamp desc limit 1;")

    res = emscur.fetchall()

    if len(res) > 0:

        if res[0][2] == "DCHG":
            sts = 'Discharge'
        if res[0][2] == "CHGRW":
            sts = 'Charge Recirculation'
        if res[0][2] == "CHGFW":
            sts = 'Charge Freshwater'
        if res[0][2] == "IDLE":
            sts = 'IDLE'
        
        storedWaterTemp = res[0][0]

        Mass = res[0][1]
    else:
        sts = None
        storedWaterTemp = None
        Mass = None

    awscur.execute("SELECT chiller1 FROM EMS.ChillerStatus where date(polledTime) = curdate() order by polledTime desc limit 1;")

    chres = awscur.fetchall()

    if len(chres) > 0:
        if chres[0][0] != None:
            if chres[0][0] == '1':
                ChSts = 'ON'
            else:
                ChSts = 'OFF'
        else:
            ChSts = 'OFF'
    else:
        ChSts = None

    hotWater_list.append({'storedWaterTemp':storedWaterTemp,'Mass':Mass,'hotWaterStatus':sts,'ChillerStatus':ChSts})

    return hotWater_list
    


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5005)