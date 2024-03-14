from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from fastapi.responses import JSONResponse

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

def get_emsdb():
    db = mysql.connector.connect(
        host="43.205.196.66",
        user="emsroot",
        password="22@teneT",
        database='EMS',
        port=3307
    )
    return db

def get_meterdb():
    db=mysql.connector.connect(
        host="43.205.196.66",
        user="emsroot",
        password="22@teneT",
        database='meterdata',
        port=3307
    )
    return db

def get_RAWemsdb():
    db=mysql.connector.connect(
        host="121.242.232.211",
        user="emsroot",
        password="22@teneT",
        database='EMS',
        port=3306
    )
    return db


@app.get('/Dashboard/REprofile')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    RE_list = []
    curRE = 0
    prevRE = 0
    Roof = 0

    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    emscur = processed_db.cursor()
    
    emscur.execute("select polledDate,sum(gridEnergy),sum(wheeledinEnergy),sum(rooftopEnergy),sum(deisel) from EMS.buidingConsumptionDayWise where polledDate <= curdate() and polledDate >= date_sub(curdate(),interval 6 day);")

    curres = emscur.fetchall()

    for i in curres:
        try:
            curRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
        except:
            continue

        Roof = (i[3] / (i[3]+i[2]))*100

        Wheeled = (i[2] / (i[2]+i[3]))*100


    emscur.execute("select polledDate,sum(gridEnergy),sum(wheeledinEnergy),sum(rooftopEnergy),sum(deisel)  from EMS.buidingConsumptionDayWise where polledDate <= date_sub(curdate(),interval 7 day) and polledDate >= date_sub(curdate(),interval 13 day);")

    prevres = emscur.fetchall()

    for i in prevres:
        try:
            prevRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
        except:
            continue

    emscur.execute("select polledDate,gridEnergy,wheeledinEnergy,rooftopEnergy,deisel from EMS.buidingConsumptionDayWise where polledDate <= curdate() and polledDate >= date_sub(curdate(),interval 29 day);")

    curmres = emscur.fetchall()

    curmRE = 0
    Roofm = 0
    Wheeledm = 0
    prevmRE = 0

    for i in curmres:
        try:
            curmRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
        except:
            continue

        Roofm = (i[3] / (i[3]+i[2]))*100

        Wheeledm = (i[2] / (i[2]+i[3]))*100

    emscur.execute("select polledDate,gridEnergy,wheeledinEnergy,rooftopEnergy,deisel from EMS.buidingConsumptionDayWise where polledDate <= date_sub(curdate(),interval 30 day) and polledDate >= date_sub(curdate(),interval 59 day);")

    prevmres = emscur.fetchall()

    for i in prevmres:
        try:
            prevmRE = ((i[2]+i[3])/(i[1]+i[2]+i[3]+i[4]))*100
        except:
            continue

    RE_list.append({"CurrentWeek":round(curRE,1),"RoofWeek":round(Roof),"WheeledWeek":round(Wheeled),"diffWeek":round(curRE - prevRE,1),
                    "CurrentMonth":round(curmRE,1),"RoofMont":round(Roofm),"WheeledMonth":round(Wheeledm),"diffMonth":round(curmRE - prevmRE,1)})

    emscur.close()

    return RE_list


@app.get('/Dashboard/Highlights')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    Highlights = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT round(sum(gridEnergy)),round(sum(wheeledinEnergy)),round(sum(rooftopEnergy)) FROM EMS.buildingConsumption where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    grid = res[0][0]
    wheeled = res[0][1]
    rooftop = res[0][2]

    bms_cur.execute("SELECT round(total_energy_difference) FROM meterdata.diselenergy where date(polled_time) = curdate() order by polled_time desc limit 1;")

    diselres = bms_cur.fetchall()

    diesel = diselres[0][0]

    bms_cur.execute("SELECT avgpowerfactor,minpowerfactor FROM EMS.schneider7230processed where date(polledTime) = curdate() order by polledTime desc limit 1;")
    
    powerres = bms_cur.fetchall()

    avgfac = powerres[0][0]
    minfac = powerres[0][1]

    Highlights.append({'wheeled':wheeled,'rooftop':rooftop,'grid':grid,
                       'diesel':diesel,'avgFactor':avgfac,'minFactor':minfac})
    
    return Highlights


@app.get('/Dashboard/WheeledInSolar')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    WheeledInSolar_response = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(Energy),sum(irradiation) FROM EMS.WheeledHourly where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()
    print(res)


    specificyield = res[0][0]/1075.8
    performancePercentage = res[0][0]/(res[0][1]/4000)*1075.8

    if performancePercentage > 100:
        performancePercentage = 81

        WheeledInSolar_response.append({'meterenergy_diff_sum':round(res[0][0]),'wmsirradiation_sum':round(res[0][1],2),
                              'performance':performancePercentage,'specific_yield':round(specificyield,2)})
    bms_cur.close()
    processed_db.close()

    return WheeledInSolar_response

#Rooftop_Solar  card api

@app.get('/Dashboard/RoofTopSolar')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    RoofTop_response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(energy),sum(irradiation) FROM EMS.roofTopHour where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    specificyield = res[0][0]/1075.8
    performancePercentage = res[0][0]/(res[0][1]/4000)*1075.8

    if performancePercentage > 100:
        performancePercentage = 81

    RoofTop_response.append({'cumulative_energy':round(res[0][0]),'sensorradiation':round(res[0][1],2),
                              'performance':performancePercentage,'specific_yield':round(specificyield,2)})
    
    bms_cur.close()
    processed_db.close()

    return RoofTop_response

#c02 reduction api

@app.get('/Dashboard/co2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    Co2_response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select sum(energy) from EMS.roofTopHour where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()
    rooftop = res[0][0]

    bms_cur.execute("SELECT sum(Energy) FROM EMS.WheeledHourly where date(polledTime) = curdate();")

    res1 = bms_cur.fetchall()
    wheeled = res1[0][0]

    co2 = [{'co2reduced':round(((rooftop+wheeled)/1000)*0.71,2)}]

    bms_cur.close()
    processed_db.close()

    return co2


# Dashboard Grid  api
@app.get('/Dashboard/Grid')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    Grid_response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT sum(Energy) as GridEnergy FROM EMS.Gridhourly where date(polledTime)=curdate();")
    
    res = bms_cur.fetchall()

    print(res)

    for i in res:
        Grid_response.append({'GridEnergy':round(i[0])})
    
    bms_cur.close()
    processed_db.close()

    return Grid_response


# Dashboard PowerFactor api
@app.get('/Dashboard/PowerFactor')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    powerFactor = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT avgpowerfactor,minpowerfactor FROM EMS.schneider7230processed where date(polledTime) = curdate() order by polledTime desc limit 1;")
    
    res = bms_cur.fetchall()
    print(res)

    for i in res:
        powerFactor.append({'average_powerfactor':i[0],'minimum_powerfactor':i[1]})
    
    bms_cur.close()
    processed_db.close()

    return powerFactor


@app.get('/Dashboard/ThermalStorage')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[0])[11:16]
        thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})

    return thermalStorage


@app.get('/Dashboard/chillerstatus')
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


@app.post('/Dashboard/ThermalStorage/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = '{value}';")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})    
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return thermalStorage


@app.get('/Dashboard/ltoBattery')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT chargingEnergy,dischargingEnergy,energyAvailable,packsoc,polledTime FROM EMS.LTObatteryHourly where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[4])[11:16]
        if i[0] != None or i[1] != None:
            lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
        else:
            lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})

    
    bms_cur.close()
    processed_db.close()
    
    return lto_list


@app.post('/Dashboard/ltoBattery/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT chargingEnergy,dischargingEnergy,energyAvailable,packsoc,polledTime FROM EMS.LTObatteryHourly where date(polledTime) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[4])[11:16]
                    if i[0] != None or i[1] != None:
                        lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
                    else:
                        lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return lto_list


@app.get('/Dashboard/upsBattery')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT chargingEnergy,discharhingEnergy,energyAvailable,packsoc,polledTime FROM EMS.UPSbatteryHourly where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[4])[11:16]
        if i[0] != None or i[1] != None:
            lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
        else:
            lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})

    
    bms_cur.close()
    processed_db.close()
    
    return lto_list



@app.post('/Dashboard/upsBattery/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    lto_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT chargingEnergy,discharhingEnergy,energyAvailable,packsoc,polledTime FROM EMS.UPSbatteryHourly where date(polledTime) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[4])[11:16]
                    if i[0] != None or i[1] != None:
                        lto_list.append({'chargingEnergy':i[0],'dischargingEnergy':i[1],'idleEnergy':0,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})
                    else:
                        lto_list.append({'chargingEnergy':0,'dischargingEnergy':0,'idleEnergy':0.01,'energy_available':i[2],'Pacsoc':i[3],'polledTime':polledTime})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return lto_list


#Evcharger api
@app.get("/dashboard/EvCharger")
def ev_charger_dashboard(db: mysql.connector.connect = Depends(get_emsdb)):
    EVCharger_list = []
    try:
        # Get data from the database
        processed_db = get_emsdb()
        charger_cur = processed_db.cursor()

        charger_cur.execute("SELECT chargpointname,energyconsumption,totalsessions FROM evcharger WHERE DATE(servertime) = CURDATE();")
        result = charger_cur.fetchall()

        # Close the database connection
        charger_cur.close()
        processed_db.close()

        # Initialize variables
        NoOfChargers = 0
        CP1_1Status = ""
        LEV4_1Status = ""
        CP11_1Status = ""
        CP12_1Status = ""
        CP13_1Status = ""
        CP14_1Status = ""

        # Initialize dictionaries for each charger
        CP1_1 = {'CP1_1Energy': 0, 'CP1_1TotalSession': 0, 'CP1_1NoOf_chargers': 0}
        LEV4_1 = {'LEV4_1Energy': 0, 'LEV4_1TotalSession': 0, 'LEV4_NoOf_chargers': 0}
        CP11_1 = {'CP11_1Energy': 0, 'CP11_1TotalSession': 0, 'CP11_1NoOf_chargers': 0}
        CP12_1 = {'CP12_1Energy': 0, 'CP12_1TotalSession': 0, 'CP12_1NoOf_chargers': 0}
        CP13_1 = {'CP13_1Energy': 0, 'CP13_1TotalSession': 0, 'CP13_1NoOf_chargers': 0}
        CP14_1 = {'CP14_1Energy': 0, 'CP14_1TotalSession': 0, 'CP14_1NoOf_chargers': 0}

        # Process the result and update dictionaries
        for entry in result:
            charger_name = entry[0]
            energy_consumption = float(entry[1])

            if charger_name == 'CP1_1':
                CP1_1['CP1_1Energy'] += energy_consumption
                CP1_1['CP1_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP1_1['CP1_1NoOf_chargers'] = 'Active'
                    CP1_1Status = 'active'
                    NoOfChargers += 1

            if charger_name == 'LEV4_1':
                LEV4_1['LEV4_1Energy'] += energy_consumption
                LEV4_1['LEV4_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    LEV4_1['LEV4_1NoOf_chargers'] = 'Active'
                    LEV4_1Status = 'active'
                    NoOfChargers += 1

            if charger_name == 'CP11_1':
                CP11_1['CP11_1Energy'] += energy_consumption
                CP11_1['CP11_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP11_1['CP11_1NoOf_chargers'] = 'Active'
                    CP11_1Status = 'active'
                    NoOfChargers += 1
            
            if charger_name == 'CP12_1':
                CP12_1['CP12_1Energy'] += energy_consumption
                CP12_1['CP12_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP12_1['CP12_1NoOf_chargers'] = 'Active'
                    CP12_1Status = 'active'
                    NoOfChargers += 1
            
            if charger_name == 'CP13_1':
                CP13_1['CP13_1Energy'] += energy_consumption
                CP13_1['CP13_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP13_1['CP13_1NoOf_chargers'] = 'Active'
                    CP13_1Status = 'active'
                    NoOfChargers += 1

            if charger_name == 'CP14_1':
                CP14_1['CP14_1Energy'] += energy_consumption
                CP14_1['CP14_1TotalSession'] = entry[2]
                if energy_consumption > 0:
                    CP14_1['CP14_1NoOf_chargers'] = 'Active'
                    CP14_1Status = 'active'
                    NoOfChargers += 1

        # Calculate total energy and total sessions
        total_energy = CP1_1['CP1_1Energy'] + LEV4_1['LEV4_1Energy'] + CP11_1['CP11_1Energy'] + CP12_1['CP12_1Energy'] + CP13_1['CP13_1Energy'] + CP14_1['CP14_1Energy']
        total_sessions = CP1_1['CP1_1TotalSession'] + LEV4_1['LEV4_1TotalSession'] + CP11_1['CP11_1TotalSession'] + CP12_1['CP12_1TotalSession'] + CP13_1['CP13_1TotalSession'] + CP14_1['CP14_1TotalSession']

        # Create the final result dictionary
        EVCharger_list.append({
            "totalEnergy": round(total_energy, 1),
            "totalSessions": total_sessions,
            "NoOfChargersUsed": NoOfChargers,
            "CP1_1Status": CP1_1Status,
            "LEV4_1Status": LEV4_1Status,
            "CP11_1Status": CP11_1Status,
            "CP12_1Status": CP12_1Status,
            "CP13_1Status": CP13_1Status,
            "CP14_1Status": CP14_1Status,
            "CP1_1EnergyConsumed": CP1_1['CP1_1Energy'],
            "LEV4_1EnergyConsumed": LEV4_1['LEV4_1Energy'],
            "CP11_1EnergyConsumed": CP11_1['CP11_1Energy'],
            "CP12_1EnergyConsumed": CP12_1['CP12_1Energy'],
            "CP13_1EnergyConsumed": CP13_1['CP13_1Energy'],
            "CP14_1EnergyConsumed": CP14_1['CP14_1Energy'],
            "CP12_1Location":"Pond area",
            "CP13_1Location":"Pond area",
            "CP14_1Location":"Pond area",
            "LEV4_1Location":"MLCP 3rd floor",
            "CP12_1Capacity":"3.3kW",
            "CP13_1Capacity":"3.3kW",
            "CP14_1Capacity":"7kW",
            "LEV4_1Capacity":"7kW"

            # Repeat the same for other chargers...
        })

        return EVCharger_list

    except Exception as e:
        # Handle exceptions and return an HTTP 500 response
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    
 #Chillers Api   
@app.get('/Dashboard/thermal')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = curdate();")
    
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[0])[11:16]
        thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})

    return thermalStorage

@app.post('/Dashboard/thermal/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    thermalStorage = []
    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:

                bmscur.execute(f"select polledTime,coolingEnergy from EMS.ThermalHourly where date(polledTime) = '{value}';")
                
                res = bmscur.fetchall()
                
                for i in res:
                    polledTime = str(i[0])[11:16]
                    thermalStorage.append({'coolingEnergy':round(i[1],2),'polledTime':polledTime})
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return thermalStorage


#Hot Water Storage api
@app.get('/Dashboard/HotWaterStorage')
def peak_demand_date(db: mysql.connector.connect = Depends(get_RAWemsdb)):
    HotWaterStorage_Response=[]
    try:
        processed_db = get_RAWemsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT recordtimestamp,tankBottomTemperature,tankTopTemperature,hotWaterDeliveryTemperature,hotWaterdeliveryRate,tankFuildVolume FROM EMS.HotWaterStorage where date(recordtimestamp)=curdate() order by recordtimestamp desc limit 1")
    
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Stored_Water_Temperature=(i[1]+i[2])/2
        Delivery_Temperature=i[3]
        Hot_water_delivery_Flow_rate=i[4]
        Energy_Delivered=(Hot_water_delivery_Flow_rate*4.2*(Stored_Water_Temperature-Delivery_Temperature))
        Mass_of_stored_water=i[5]
        Refrigerant_temperature=55
        Energy_Stored=0
         
        
        HotWaterStorage_Response.append({
      "Stored_Water_Temperature":Stored_Water_Temperature,
      "Delivery_Temperature":Delivery_Temperature,
      "Hot_water_delivery_Flow_rate":Hot_water_delivery_Flow_rate,
      "Energy_Delivered":Energy_Delivered,
      "Mass_of_stored_water":Mass_of_stored_water,
      "Refrigerant_temperature":Refrigerant_temperature,
      "Energy_Stored":Energy_Stored,
    })

    return HotWaterStorage_Response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5002)
