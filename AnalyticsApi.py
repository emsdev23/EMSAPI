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
        host="121.242.232.211",
        user="emsroot",
        password="22@teneT",
        database='EMS',
        port=3306
    )
    return db

def get_awsdb():
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


@app.get('/IoeBattery/EnergyVsPacksoc')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    energyPack = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    awscur = processed_db.cursor()

    awscur.execute("""SELECT 	polledTime,Energyst1,Energyst2,Energyst3,Energyst4,Energyst5,
                                packSocst1,packSocst2,packSocst3,packSocst4,packSocst5
                        FROM 	EMS.ioeMinWise 
                        where 	date(polledTime) = curdate();""")
    
    res = awscur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        Energyst1 = i[1]
        Energyst2 = i[2]
        Energyst3 = i[3]
        Energyst4 = i[4]
        Energyst5 = i[5]
        packSocst1 = i[6]
        packSocst2 = i[7]
        packSocst3 = i[8]
        packSocst4 = i[9]
        packSocst5 = i[10]

        energyPack.append({'polledTime':polledTime,'Energyst1':Energyst1,'Energyst2':Energyst2,'Energyst3':Energyst3,
                           'Energyst4':Energyst4,'Energyst5':Energyst5,'packSocst1':packSocst1,'packSocst2':packSocst2,
                           'packSocst3':packSocst3,'packSocst4':packSocst4,'packSocst5':packSocst5})

    return energyPack

@app.post('/IoeBattery/EnergyVsPacksoc/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    energyPack = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:
                bms_cur.execute(f"""SELECT 	polledTime,Energyst1,Energyst2,Energyst3,Energyst4,Energyst5,
                                            packSocst1,packSocst2,packSocst3,packSocst4,packSocst5
                                    FROM 	EMS.ioeMinWise 
                                    where 	date(polledTime) ='{value}';""")

                res = bms_cur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    Energyst1 = i[1]
                    Energyst2 = i[2]
                    Energyst3 = i[3]
                    Energyst4 = i[4]
                    Energyst5 = i[5]
                    packSocst1 = i[6]
                    packSocst2 = i[7]
                    packSocst3 = i[8]
                    packSocst4 = i[9]
                    packSocst5 = i[10]

                    energyPack.append({'polledTime':polledTime,'Energyst1':Energyst1,'Energyst2':Energyst2,'Energyst3':Energyst3,
                                    'Energyst4':Energyst4,'Energyst5':Energyst5,'packSocst1':packSocst1,'packSocst2':packSocst2,
                                    'packSocst3':packSocst3,'packSocst4':packSocst4,'packSocst5':packSocst5})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return energyPack

@app.get('/chillerDashboard/ChillerLoading')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    ChillerLoading_PH1_PH2=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT c1loading,c2loading,c3loading,c4loading,lastTimestamp FROM meterdata.chillarloading where date(lastTimestamp)=curdate()")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[4])[11:16]
        if(i[0]==None):
            c1loading=0
        else:
            c1loading=round(i[0],2)

        if(i[1]==None):
            c2loading=0
        else:
            c2loading=round(i[1],2)

        if(i[2]==None):
            c3loading=0
        else:
            c3loading=round(i[2],2)

        if(i[3]==None):
            c4loading=0
        else:
            c4loading=round(i[3],2)

    bms_cur.execute("SELECT c5loading,c6loading,c7loading,c8loading,timestamp FROM meterdata.chillarloading5678 where date(timestamp)=curdate();")
    res = bms_cur.fetchall()
    for i in res:
        polledTime = str(i[4])[11:16]
        if(i[0]==None):
            c5loading=0
        else:
            c5loading=round(i[0],2)

        if(i[1]==None):
            c6loading=0
        else:
            c6loading=round(i[1],2)

        if(i[2]==None):
            c7loading=0
        else:
            c7loading=round(i[2],2)

        if(i[3]==None):
            c8loading=0
        else:
            c8loading=round(i[3],2)


        ChillerLoading_PH1_PH2.append({'Phase2c1loading':c1loading,'Phase2c2loading':c2loading,'Phase2c3loading':c3loading,'Phase2c4loading':c4loading,'Phase1c5loading':c5loading,'Phase1c6loading':c6loading,'Phase1c7loading':c7loading,'Phase1c8loading':c8loading,'polledTime':polledTime})
   

    return ChillerLoading_PH1_PH2


@app.get('/Deisel/analytics/graph')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    diesel_list = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledTime,dg1Energy,dg2Energy,dg3Energy,dg4Energy,dg5Energy FROM EMS.dieselQuarterly where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]

        diesel_list.append({'Timestamp':polledTime,'DGNum_1_energy_difference':i[1],'DGNum_2_energy_difference':i[2],'DGNum_3_energy_difference':i[3],
                            'DGNum_4_energy_difference':i[4],'DGNum_5_energy_difference':i[5]})
    
    return diesel_list


@app.post('/Deisel/analytics/graph/DateFilter')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    diesel_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:
                bms_cur.execute(f"SELECT polledTime,dg1Energy,dg2Energy,dg3Energy,dg4Energy,dg5Energy FROM EMS.dieselQuarterly where date(polledTime) = '{value}';")

                res = bms_cur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]

                    diesel_list.append({'Timestamp':polledTime,'DGNum_1_energy_difference':i[1],'DGNum_2_energy_difference':i[2],'DGNum_3_energy_difference':i[3],
                                        'DGNum_4_energy_difference':i[4],'DGNum_5_energy_difference':i[5]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return diesel_list


@app.get('/fiveminWise')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    min_list = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledTime,wheeled,grid FROM EMS.fiveMinData where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        min_list.append({'polledTime':polledTime,'wheeledEnergy':i[1],'gridEnergy':i[2]})

    return min_list

@app.post('/filtered/fiveminWise')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    min_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT polledTime,wheeled,grid FROM EMS.fiveMinData where date(polledTime) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    min_list.append({'polledTime':polledTime,'wheeledEnergy':i[1],'gridEnergy':i[2]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return min_list


@app.get('/BuildingConsumption/BlockWise')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    BlockWise_Response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT timestamp,ABLOCK,BBlock,CBLOCK,DBLOCK,EBLOCK,MLCP,Utility,auditorium FROM meterdata.BlockwiseDaywise where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    polledTime = str(res[0][0])[8:10]+"/"+str(res[0][0])[5:7]+"/"+str(res[0][0])[0:4]

    print(polledTime)

    if res[0][1] != None:
        ablock = round(res[0][1])
    else:
        ablock = 0

    if res[0][2] != None:
        bblock = round(res[0][2])
    else:
        bblock = 0

    if res[0][3] != None:
        cblock = round(res[0][3])
    else:
        cblock = 0

    if res[0][4] != None:
        dblock = round(res[0][4])
    else:
        dblock = 0

    if res[0][5] != None:
        eblock = round(res[0][5])
    else:
        eblock = 0
    
    if res[0][6] != None:
        mlcp = round(res[0][6])
    else:
        mlcp = 0
    
    if res[0][7] != None:
        utility = round(res[0][7])
    else:
        utility = 0
    
    if res[0][8] != None:
        audi = round(res[0][8])
    else:
        audi = 0


    BlockWise_Response.append({'timestamp':polledTime,'ABLOCK':ablock,'BBlock':bblock,'CBLOCK':cblock,'DBLOCK':dblock,'EBLOCK':eblock,'MLCP':mlcp,'Utility':utility,'auditorium':audi})
    bms_cur.close()
    processed_db.close()

    return BlockWise_Response


@app.post('/filtered/BuildingConsumption/BlockWise')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    BlockWise_Response = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT timestamp,ABLOCK,BBlock,CBLOCK,DBLOCK,EBLOCK,MLCP,Utility,auditorium FROM meterdata.BlockwiseDaywise where date(timestamp)='{value}';")

                res = bmscur.fetchall()

                polledTime = str(res[0][0])[8:10]+"/"+str(res[0][0])[5:7]+"/"+str(res[0][0])[0:4]

                print(polledTime)

                if res[0][1] != None:
                    ablock = round(res[0][1])
                else:
                    ablock = 0

                if res[0][2] != None:
                    bblock = round(res[0][2])
                else:
                    bblock = 0

                if res[0][3] != None:
                    cblock = round(res[0][3])
                else:
                    cblock = 0

                if res[0][4] != None:
                    dblock = round(res[0][4])
                else:
                    dblock = 0

                if res[0][5] != None:
                    eblock = round(res[0][5])
                else:
                    eblock = 0
                
                if res[0][6] != None:
                    mlcp = round(res[0][6])
                else:
                    mlcp = 0
                
                if res[0][7] != None:
                    utility = round(res[0][7])
                else:
                    utility = 0
                
                if res[0][8] != None:
                    audi = round(res[0][8])
                else:
                    audi = 0


                BlockWise_Response.append({'timestamp':polledTime,'ABLOCK':ablock,'BBlock':bblock,'CBLOCK':cblock,'DBLOCK':dblock,'EBLOCK':eblock,'MLCP':mlcp,'Utility':utility,'auditorium':audi})
                bmscur.close()
                db.close()

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return BlockWise_Response



def stdateFunc(dated):
    mon = dated[5:]

    if len(mon) == 1:
        stdate = dated[0:5]+"0"+mon+"-01"
    else:
        stdate = dated[0:5]+mon+"-01"

    return stdate

def enddateFunct(dated):
    mon = dated[5:]

    if mon == "2":
        dt = "-28"
    elif mon in ("1","3","5","7","8","10","12"):
        dt = "-31"
    else:
        dt = "-30"
    
    if len(mon) == 1:
        stdate = dated[0:5]+"0"+mon+dt
    else:
        stdate = dated[0:5]+mon+dt

    return stdate


@app.get('/peakMontly')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    peak_lis = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledDate,peakDemand FROM EMS.peakMonthly where polledDate >= date_sub(now(),interval 365 day) and polledDate <= curdate();")

    res = bms_cur.fetchall()

    for i in res:
        peak_lis.append({'polledDate':i[0],'peakDemand':i[1]})
    
    return peak_lis

@app.post('/filtered/peakMontly')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    peak_lis = []

    try:
        stdate = data.get('date')
        enddate = data.get('enddate')

        if stdate and enddate and isinstance(stdate, str) and isinstance(enddate, str):
            stdate = stdateFunc(stdate)
            enddate = enddateFunct(enddate)
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT polledDate,peakDemand FROM EMS.peakMonthly where date(polledDate) >='{stdate}' and date(polledDate) <= '{enddate}';")

                res = bmscur.fetchall()

                for i in res:
                    peak_lis.append({'polledDate':i[0],'peakDemand':i[1]})

        elif stdate and isinstance(stdate, str):
            stdate = stdateFunc(stdate)
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT polledDate,peakDemand FROM EMS.peakMonthly where date(polledDate) >='{stdate}' and date(polledDate) <= curdate();")

                res = bmscur.fetchall()

                for i in res:
                    peak_lis.append({'polledDate':i[0],'peakDemand':i[1]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return peak_lis


@app.get('/gridMontly')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    grid_lis = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledDate,Energy FROM EMS.gridMonthly where polledDate >= date_sub(now(),interval 365 day) and polledDate <= curdate();")

    res = bms_cur.fetchall()

    for i in res:
        grid_lis.append({'polledDate':i[0],'Energy':i[1]})
    
    return grid_lis


@app.post('/filtered/gridMonthly')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    grid_lis = []

    try:
        stdate = data.get('date')
        enddate = data.get('enddate')

        if stdate and enddate and isinstance(stdate, str) and isinstance(enddate, str):
            stdate = stdateFunc(stdate)
            enddate = enddateFunct(enddate)
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT polledDate,Energy FROM EMS.gridMonthly where date(polledDate) >='{stdate}' and date(polledDate) <= '{enddate}';")

                res = bmscur.fetchall()

                for i in res:
                    grid_lis.append({'polledDate':i[0],'Energy':i[1]})

        elif stdate and isinstance(stdate, str):
            stdate = stdateFunc(stdate)
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT polledDate,Energy FROM EMS.gridMonthly where date(polledDate) >='{stdate}' and date(polledDate) <= curdate();")

                res = bmscur.fetchall()

                for i in res:
                    grid_lis.append({'polledDate':i[0],'Energy':i[1]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return grid_lis


@app.get('/minWise')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    min_list = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledTime,wheeled,grid FROM EMS.minWiseData where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        min_list.append({'polledTime':polledTime,'wheeledEnergy':i[1],'gridEnergy':i[2]})

    return min_list


@app.post('/filtered/minWise')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    min_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT polledTime,wheeled,grid FROM EMS.minWiseData where date(polledTime) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    min_list.append({'polledTime':polledTime,'wheeledEnergy':i[1],'gridEnergy':i[2]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return min_list



@app.get('/BuildingConsumption/TopTenClients')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    TopTenClients_Response = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT ACRI,pfizer,SGRI,tatacommunications,ginger,axxlent,caterpillar,IFMR,NMS,TCS FROM meterdata.toptenclientsdaywise where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    if res[0][0] != None:
        arci = round(res[0][0]) 
    else:
        arci = 0

    if res[0][1] != None:
        pfizer = round(res[0][1]) 
    else:
        pfizer = 0

    if res[0][2] != None:
        SGRI = round(res[0][2]) 
    else:
        SGRI = 0

    if res[0][3] != None:
        tatacommunications = round(res[0][3]) 
    else:
        tatacommunications = 0

    if res[0][4] != None:
        ginger = round(res[0][4]) 
    else:
        ginger = 0
    
    if res[0][5] != None:
        axxlent = round(res[0][5]) 
    else:
        axxlent = 0
    
    if res[0][6] != None:
        caterpillar = round(res[0][6]) 
    else:
        caterpillar = 0

    if res[0][7] != None:
        IFMR = round(res[0][7]) 
    else:
        IFMR = 0
    
    if res[0][8] != None:
        NMS = round(res[0][8]) 
    else:
        NMS = 0
    
    if res[0][9] != None:
        TCS = round(res[0][9]) 
    else:
        TCS = 0


    TopTenClients_Response.append({'ACRI':arci,'pfizer':pfizer,'SGRI':SGRI,'tatacommunications':tatacommunications,'ginger':ginger,
                                   'axxlent':axxlent,'caterpillar':caterpillar,'IFMR':IFMR,
                                   'NMS':NMS,'TCS':TCS})
    bms_cur.close()
    processed_db.close()

    return TopTenClients_Response



@app.post('/BuildingConsumption/TopTenClients/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):
    TopTenClients_Response = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT ACRI,pfizer,SGRI,tatacommunications,ginger,axxlent,caterpillar,IFMR,NMS,TCS FROM meterdata.toptenclientsdaywise where date(timestamp) = '{value}'")

                res = bmscur.fetchall()
                print(res)

                if res[0][0] != None:
                    arci = round(res[0][0]) 
                else:
                    arci = 0

                if res[0][1] != None:
                    pfizer = round(res[0][1]) 
                else:
                    pfizer = 0

                if res[0][2] != None:
                    SGRI = round(res[0][2]) 
                else:
                    SGRI = 0

                if res[0][3] != None:
                    tatacommunications = round(res[0][3]) 
                else:
                    tatacommunications = 0

                if res[0][4] != None:
                    ginger = round(res[0][4]) 
                else:
                    ginger = 0
                
                if res[0][5] != None:
                    axxlent = round(res[0][5]) 
                else:
                    axxlent = 0
                
                if res[0][6] != None:
                    caterpillar = round(res[0][6]) 
                else:
                    caterpillar = 0

                if res[0][7] != None:
                    IFMR = round(res[0][7]) 
                else:
                    IFMR = 0
                
                if res[0][8] != None:
                    NMS = round(res[0][8]) 
                else:
                    NMS = 0
                
                if res[0][9] != None:
                    TCS = round(res[0][9]) 
                else:
                    TCS = 0


                TopTenClients_Response.append({'ACRI':arci,'pfizer':pfizer,'SGRI':SGRI,'tatacommunications':tatacommunications,'ginger':ginger,
                                            'axxlent':axxlent,'caterpillar':caterpillar,'IFMR':IFMR,
                                            'NMS':NMS,'TCS':TCS})
                
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return TopTenClients_Response


@app.get('/Analytics/rooftopSolar')
def peak_demand_date(db: mysql.connector.connect = Depends(get_awsdb)):
    roofTop_solar = []
    try:
        processed_db = get_awsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT polledTime,energy,irradiation,expph1Energy,expph1Energy,ph1Actual,ph2Actual FROM EMS.roofTopHour where date(polledTime) = curdate();")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        roofTop_solar.append({'polledTime':polledTime,'Energy':i[1],'irradiation':i[2],'expph1Energy':i[3],
                              'expph2Energy':i[4],'ph1Actual':i[5],'ph2Actual':i[6]})
        
    return roofTop_solar


@app.post('/Analytics/rooftopSolar/filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    roofTop_solar = []
    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:

                bms_cur.execute(f"SELECT polledTime,energy,irradiation,expph1Energy,expph1Energy,ph1Actual,ph2Actual FROM EMS.roofTopHour where date(polledTime) = '{value}';")

                res = bms_cur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    roofTop_solar.append({'polledTime':polledTime,'Energy':i[1],'irradiation':i[2],'expph1Energy':i[3],
                                        'expph2Energy':i[4],'ph1Actual':i[5],'ph2Actual':i[6]})
                    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)
    
    return roofTop_solar

@app.get('/Upsanalytics/energy_VS_packsoc')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    ups_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""SELECT MIN(pack_usable_soc) AS pack_usable_soc,
                            MAX(received_time) AS received_time,
                            MAX(upsbatterystatus) AS upsbatterystatus,
                            MAX(upschargingenergy) AS upschargingenergy,
                            MAX(upsdischargingenergy) AS upsdischargingenergy,
                            HOUR(received_time) AS hr,
                            MINUTE(received_time) AS mint
                        FROM 
                            EMS.EMSUPSbattery
                        WHERE 
                            DATE(received_time) = DATE_SUB(CURDATE(), INTERVAL 150 DAY)
                        GROUP BY 
                            hr, mint;""")
    
    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[1])[11:16]
        if i[2] == 'IDLE':
            batteryEnergy = 0.01
        elif i[2] == 'CHG':
            batteryEnergy = i[3]/100
        elif i[2] == 'DCHG':
            batteryEnergy = i[4]/100
        ups_list.append({'packsoc':i[0],'batteryEnergy':batteryEnergy,'timestamp':polledTime,'batteryStatus':i[2]})

    bms_cur.close()
    processed_db.close()

    return ups_list   


@app.post('/filtered/Upsanalytics/energy_VS_packsoc')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    ups_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute("""SELECT MIN(pack_usable_soc) AS pack_usable_soc,
                                        MAX(received_time) AS received_time,
                                        MAX(upsbatterystatus) AS upsbatterystatus,
                                        MAX(upschargingenergy) AS upschargingenergy,
                                        MAX(upsdischargingenergy) AS upsdischargingenergy,
                                        HOUR(received_time) AS hr,
                                        MINUTE(received_time) AS mint
                                    FROM 
                                        EMS.EMSUPSbattery
                                    WHERE 
                                        DATE(received_time) = DATE_SUB(CURDATE(), INTERVAL 150 DAY)
                                    GROUP BY 
                                        hr, mint;""")
                
                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[1])[11:16]
                    if i[2] == 'IDLE':
                        batteryEnergy = 0.01
                    elif i[2] == 'CHG':
                        batteryEnergy = i[3]/100
                    elif i[2] == 'DCHG':
                        batteryEnergy = i[4]/100
                    
                    ups_list.append({'packsoc':i[0],'batteryEnergy':batteryEnergy,'timestamp':polledTime,'batteryStatus':i[2]})
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return ups_list


@app.get('/Upsanalytics/current_VS_voltage')
def peak_demand_date(db: mysql.connector.connect = Depends(get_emsdb)):
    ups_list = []
    try:
        processed_db = get_emsdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""SELECT max(received_time),max(batteryvoltage),max(batterycurrent),
                                hour(received_time) as hr,minute(received_time) as mint 
                                FROM EMS.EMSUPSbattery
                                where date(received_time) = curdate() 
                                group by hr,mint;""")
    
    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[0])[11:16]
        ups_list.append({'polledTime':polledTime,'BatteryVoltage':i[1],'BatteryCurrent':i[2]})

    bms_cur.close()
    processed_db.close()

    return ups_list


@app.post('/filtered/Upsanalytics/current_VS_voltage')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_emsdb)):
    ups_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT max(received_time),max(batteryvoltage),max(batterycurrent),hour(received_time) as hr,minute(received_time) as mint FROM EMS.EMSUPSbattery where date(received_time) = '{value}' group by hr,mint;")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    ups_list.append({'polledTime':polledTime,'BatteryVoltage':i[1],'BatteryCurrent':i[2]})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": ["MySQL connection error",e]}, status_code=500)

    return ups_list


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5003)





