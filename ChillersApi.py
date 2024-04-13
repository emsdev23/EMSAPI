from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from fastapi.responses import JSONResponse
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

def get_meterdb():
    db=mysql.connector.connect(
        host=data['awsDB']['host'],
        user=data['awsDB']['user'],
        password=data['awsDB']['password'],
        database='meterdata',
        port=data['awsDB']['port']
    )
    return db


@app.get('/chillerDashboard/CTloadvscop')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    ctcop_list = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("""select c1load,c2load,c3load,c4load,c5load,c6load,c7load,c8load,
c1cop,c2cop,c3cop,c4cop,c5cop,c6cop,c6cop,c8cop,
polledTime from EMS.cloadVScop where date(polledTime) = curdate();""")

    res = bms_cur.fetchall()

    for i in res:
        polledTime = str(i[16])[11:16]
        ctcop_list.append({"c1load":i[0],"c2load":i[1],"c3load":i[2],"c4load":i[3],"c5load":i[4],"c6load":i[5],
                           "c7load":i[6],"c8load":i[7],"c1cop":i[8],"c2cop":i[9],"c3cop":i[10],"c4cop":i[11],"c5cop":i[12],
                           "c6cop":i[13],"c7cop":i[14],"c8cop":i[15],"polledTime":polledTime})
        
    return ctcop_list



@app.post('/chillerDashboard/CTloadvscop/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):
    ctcop_list = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bms_cur:
                bms_cur.execute("""select c1load,c2load,c3load,c4load,c5load,c6load,c7load,c8load,
                c1cop,c2cop,c3cop,c4cop,c5cop,c6cop,c6cop,c8cop,
                polledTime from EMS.cloadVScop where date(polledTime) = curdate();""")

                res = bms_cur.fetchall()

                for i in res:
                    polledTime = str(i[16])[11:16]
                    ctcop_list.append({"c1load":i[0],"c2load":i[1],"c3load":i[2],"c4load":i[3],"c5load":i[4],"c6load":i[5],
                                        "c7load":i[6],"c8load":i[7],"c1cop":i[8],"c2cop":i[9],"c3cop":i[10],"c4cop":i[11],"c5cop":i[12],
                                        "c6cop":i[13],"c7cop":i[14],"c8cop":i[15],"polledTime":polledTime})

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)
    
    return ctcop_list


@app.get('/chillerDashboard/CTperformance')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    ct_list = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
    
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT CT5,CT6,CT7,CT8,CT9 FROM meterdata.ctperform2 where date(polledTime) = curdate() order by polledTime limit 1;")
   
    res2 = bms_cur.fetchall()

    bms_cur.execute("SELECT CT1,CT2,CT3,CT4 FROM meterdata.ctperform1 where date(polledTime) = curdate() order by polledTime limit 1;")
   
    res1 = bms_cur.fetchall()

    if len(res1) > 0 and len(res2) >0:
        j = res1[0]
        i = res2[0]
        if i[0] == None:
            CT5 = 0
        else:
            CT5 = i[0]
        if i[1] == None:
            CT6 = 0
        else:
            CT6 = i[1]
        if i[2] == None:
            CT7 = 0 
        else:
            CT7 = i[2]
        if i[3] == None:
            CT8 = 0
        else:
            CT8 = i[3] 
        if i[4] == None:
            CT9 = 0
        else:
            CT9 = i[4]
        if j[0] == None:
            CT1 = 0
        else:
            CT1 = j[0]
        if j[1] == None:
            CT2 = 0
        else:
            CT2 = j[1]
        if j[2] == None:
            CT3 = 0
        else:
            CT3 = j[2]
        if j[3] == None:
            CT4 = 0
        else:
            CT4 = j[3]
        ct_list.append({"CT1":CT1,"CT2":CT2,"CT3":CT3,"CT4":CT4,"CT5":CT5,"CT6":CT6,"CT7":CT7,"CT8":CT8,"CT9":CT9})
    elif len(res1) > 0 and len(res2) == 0:
        if j[0] == None:
            CT1 = 0
        else:
            CT1 = j[0]
        if j[1] == None:
            CT2 = 0
        else:
            CT2 = j[1]
        if j[2] == None:
            CT3 = 0
        else:
            CT3 = j[2]
        if j[3] == None:
            CT4 = 0
        else:
            CT4 = j[3]
        j = res1[0]
        ct_list.append({"CT1":CT1,"CT2":CT2,"CT3":CT3,"CT4":CT4,"CT5":0,"CT6":0,"CT7":0,"CT8":0,"CT9":0})
    elif len(res1) == 0 and len(res2) > 0:
        i = res2[0]
        if i[0] == None:
            CT5 = 0
        else:
            CT5 = i[0]
        if i[1] == None:
            CT6 = 0
        else:
            CT6 = i[1]
        if i[2] == None:
            CT7 = 0 
        else:
            CT7 = i[2]
        if i[3] == None:
            CT8 = 0
        else:
            CT8 = i[3] 
        if i[4] == None:
            CT9 = 0
        else:
            CT9 = i[4]
        ct_list.append({"CT1":0,"CT2":0,"CT3":0,"CT4":0,"CT5":CT5,"CT6":CT6,"CT7":CT7,"CT8":CT8,"CT9":CT9})
    else:
        ct_list.append({"CT1":0,"CT2":0,"CT3":0,"CT4":0,"CT5":0,"CT6":0,"CT7":0,"CT8":0,"CT9":0})

    return ct_list

@app.get('/chillerDashboard/thermal/storedWaterTemp')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    storedWaterTemp = []
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT timecolumn,storedwatertemperature FROM meterdata.thermalstoredwaterquaterly where date(timecolumn)=curdate()")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        polledTime = str(i[0])[11:16]
        storedWaterTemp.append({'storedwatertemperature':round(i[1],2),'polledTime':polledTime})

    return storedWaterTemp


@app.post('/chillerDashboard/thermal/storedWaterTemp/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):
    StoredWaterTemp = []

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT timecolumn,storedwatertemperature FROM meterdata.thermalstoredwaterquaterly where date(timecolumn) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    polledTime = str(i[0])[11:16]
                    StoredWaterTemp.append({'storedwatertemperature':round(i[1],2),'polledTime':polledTime})
       
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

    return StoredWaterTemp

#------------------------- Stored Water Temparature






#------------------------- Chiller Loading
@app.get('/chillerDashboard/ChillerLoading/Phase2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    ChillerLoading_PH2=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT c1loading,c2loading,c3loading,c4loading,lastTimestamp FROM meterdata.chillarloading where date(lastTimestamp)=curdate()")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Phase2polledTime = str(i[4])[11:16]
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

        ChillerLoading_PH2.append({
              'Phase2c1loading':c1loading,
              'Phase2c2loading':c2loading,
              'Phase2c3loading':c3loading,
              'Phase2c4loading':c4loading,
              'Phase2polledTime':Phase2polledTime
            })
    return ChillerLoading_PH2


@app.get('/chillerDashboard/ChillerLoading/Phase1')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):
    ChillerLoading_PH1=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT c5loading,c6loading,c7loading,c8loading,timestamp FROM meterdata.chillarloading5678 where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Phase1polledTime = str(i[4])[11:16]
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

        ChillerLoading_PH1.append({
            'Phase1c5loading':c5loading,
            'Phase1c6loading':c6loading,
            'Phase1c7loading':c7loading,
            'Phase1c8loading':c8loading,
            'Phase1polledTime':Phase1polledTime
         })
    return ChillerLoading_PH1

@app.post('/chillerDashboard/ChillerLoading/Phase2/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    ChillerLoading_PH2=[]

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT c1loading,c2loading,c3loading,c4loading,lastTimestamp FROM meterdata.chillarloading where date(lastTimestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    Phase2polledTime = str(i[4])[11:16]
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
                    ChillerLoading_PH2.append({
                        'Phase2c1loading': c1loading,
                        'Phase2c2loading': c2loading,
                        'Phase2c3loading': c3loading,
                        'Phase2c4loading': c4loading,
                        'Phase2polledTime': Phase2polledTime
                    })
       
            return ChillerLoading_PH2
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)



@app.post('/chillerDashboard/ChillerLoading/Phase1/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    ChillerLoading_PH1=[]

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT c5loading,c6loading,c7loading,c8loading,timestamp FROM meterdata.chillarloading5678 where date(timestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    Phase1polledTime = str(i[4])[11:16]
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
                    ChillerLoading_PH1.append({
                        'Phase1c5loading': c5loading,
                        'Phase1c6loading': c6loading,
                        'Phase1c7loading': c7loading,
                        'Phase1c8loading': c8loading,
                        'Phase1polledTime': Phase1polledTime
                    })
       
            return ChillerLoading_PH1
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)



 

#------------------------- Chiller Loading





#------------------------- Thermal IN/Out let phase 1 and 2
@app.get('/chillerDashboard/thermalinletoutlet/condenser/evaporator/phase2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):

    thermalinletoutlet_PH2=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT avg_commonHeaderinletTemp as EvaporatorInletTemp, avg_commonHeaderoutletTemp as EvaporatorOutletTemp,avg_condenserLineInletTemp,avg_condenserLineOutletTemp,avg_commonHeaderFlowrate as EvaporatorFlowrate,avg_condenserLineFlowrate,Timestamp FROM meterdata.thermalinletoutlet where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Phase2polledTime = str(i[6])[11:16]
        if(i[0]==None):
            Phase2EvaporatorInletTemp=0
        else:
            Phase2EvaporatorInletTemp=round(i[0],2)

        if(i[1]==None):
            Phase2EvaporatorOutletTemp=0
        else:
            Phase2EvaporatorOutletTemp=round(i[1],2)

        if(i[2]==None):
            Phase2avg_condenserLineInletTemp=0
        else:
            Phase2avg_condenserLineInletTemp=round(i[2],2)

        if(i[3]==None):
            Phase2avg_condenserLineOutletTemp=0
        else:
            Phase2avg_condenserLineOutletTemp=round(i[3],2)

        if(i[4]==None):
            Phase2EvaporatorFlowrate=0
        else:
            Phase2EvaporatorFlowrate=round(i[4],2)
        if(i[5]==None):
            Phase2avg_condenserLineFlowrate=0
        else:
            Phase2avg_condenserLineFlowrate=round(i[5],2)
        thermalinletoutlet_PH2.append({
            'Phase2EvaporatorInletTemp': Phase2EvaporatorInletTemp,
            'Phase2EvaporatorOutletTemp':Phase2EvaporatorOutletTemp,
            'Phase2avg_condenserLineInletTemp':Phase2avg_condenserLineInletTemp,
            'Phase2avg_condenserLineOutletTemp':Phase2avg_condenserLineOutletTemp,
            'Phase2EvaporatorFlowrate':Phase2EvaporatorFlowrate,
            'Phase2avg_condenserLineFlowrate':Phase2avg_condenserLineFlowrate,
            'Phase2polledTime':Phase2polledTime,
            'EvapLowerlimit':5,
            'EvapUpperlimit':18,
            'CondLowerlimit':20,
            'CondUpperlimit':40
         })

    return thermalinletoutlet_PH2


@app.get('/chillerDashboard/thermalinletoutlet/condenser/evaporator/phase1')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):

    thermalinletoutlet_PH1=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT avg_commonHeaderinletTemp as EvaporatorInletTemp, avg_commonHeaderoutletTemp as EvaporatorOutletTemp,avg_condenserLineInletTemp,avg_condenserLineOutletTemp,avg_commonHeaderFlowrate as EvaporatorFlowrate,avg_condenserLineFlowrate,Timestamp FROM meterdata.thermalinletoutlet5678 where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Phase1polledTime = str(i[6])[11:16]
        if(i[0]==None):
            Phase1EvaporatorInletTemp=0
        else:
            Phase1EvaporatorInletTemp=round(i[0],2)

        if(i[1]==None):
            Phase1EvaporatorOutletTemp=0
        else:
            Phase1EvaporatorOutletTemp=round(i[1],2)

        if(i[2]==None):
            Phase1avg_condenserLineInletTemp=0
        else:
            Phase1avg_condenserLineInletTemp=round(i[2],2)

        if(i[3]==None):
            Phase1avg_condenserLineOutletTemp=0
        else:
            Phase1avg_condenserLineOutletTemp=round(i[3],2)

        if(i[4]==None):
            Phase1EvaporatorFlowrate=0
        else:
            Phase1EvaporatorFlowrate=round(i[4],2)
        if(i[5]==None):
            Phase1avg_condenserLineFlowrate=0
        else:
            Phase1avg_condenserLineFlowrate=round(i[5],2)
        thermalinletoutlet_PH1.append({
            'Phase1EvaporatorInletTemp': Phase1EvaporatorInletTemp,
            'Phase1EvaporatorOutletTemp':Phase1EvaporatorOutletTemp,
            'Phase1avg_condenserLineInletTemp':Phase1avg_condenserLineInletTemp,
            'Phase1avg_condenserLineOutletTemp':Phase1avg_condenserLineOutletTemp,
            'Phase1EvaporatorFlowrate':Phase1EvaporatorFlowrate,
            'Phase1avg_condenserLineFlowrate':Phase1avg_condenserLineFlowrate,
            'Phase1polledTime':Phase1polledTime,
            'EvapLowerlimit':5,
            'EvapUpperlimit':18,
            'CondLowerlimit':20,
            'CondUpperlimit':40
         })

    return thermalinletoutlet_PH1



@app.post('/chillerDashboard/thermalinletoutlet/condenser/evaporator/Phase2/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    thermalinletoutlet_PH2=[]

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT avg_commonHeaderinletTemp as EvaporatorInletTemp, avg_commonHeaderoutletTemp as EvaporatorOutletTemp,avg_condenserLineInletTemp,avg_condenserLineOutletTemp,avg_commonHeaderFlowrate as EvaporatorFlowrate,avg_condenserLineFlowrate,Timestamp FROM meterdata.thermalinletoutlet where date(Timestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    Phase2polledTime = str(i[6])[11:16]
                    if(i[0]==None):
                        Phase2EvaporatorInletTemp=0
                    else:
                        Phase2EvaporatorInletTemp=round(i[0],2)
                    if(i[1]==None):
                        Phase2EvaporatorOutletTemp=0
                    else:
                        Phase2EvaporatorOutletTemp=round(i[1],2)
                    if(i[2]==None):
                        Phase2avg_condenserLineInletTemp=0
                    else:
                        Phase2avg_condenserLineInletTemp=round(i[2],2)
                    if(i[3]==None):
                        Phase2avg_condenserLineOutletTemp=0
                    else:
                        Phase2avg_condenserLineOutletTemp=round(i[3],2)
                    if(i[4]==None):
                        Phase2EvaporatorFlowrate=0
                    else:
                        Phase2EvaporatorFlowrate=round(i[4],2)
                    if(i[5]==None):
                        Phase2avg_condenserLineFlowrate=0
                    else:
                        Phase2avg_condenserLineFlowrate=round(i[5],2)
                    thermalinletoutlet_PH2.append({
                        'Phase2EvaporatorInletTemp': Phase2EvaporatorInletTemp,
                        'Phase2EvaporatorOutletTemp':Phase2EvaporatorOutletTemp,
                        'Phase2avg_condenserLineInletTemp':Phase2avg_condenserLineInletTemp,
                        'Phase2avg_condenserLineOutletTemp':Phase2avg_condenserLineOutletTemp,
                        'Phase2EvaporatorFlowrate':Phase2EvaporatorFlowrate,
                        'Phase2avg_condenserLineFlowrate':Phase2avg_condenserLineFlowrate,
                        'Phase2polledTime':Phase2polledTime,
                        'EvapLowerlimit':5,
                        'EvapUpperlimit':18,
                        'CondLowerlimit':20,
                        'CondUpperlimit':40
                     })
       
            return thermalinletoutlet_PH2
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)



@app.post('/chillerDashboard/thermalinletoutlet/condenser/evaporator/Phase1/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    thermalinletoutlet_PH1=[]

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT avg_commonHeaderinletTemp as EvaporatorInletTemp, avg_commonHeaderoutletTemp as EvaporatorOutletTemp,avg_condenserLineInletTemp,avg_condenserLineOutletTemp,avg_commonHeaderFlowrate as EvaporatorFlowrate,avg_condenserLineFlowrate,Timestamp FROM meterdata.thermalinletoutlet5678 where date(timestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    Phase1polledTime = str(i[6])[11:16]
                    if(i[0]==None):
                        Phase1EvaporatorInletTemp=0
                    else:
                        Phase1EvaporatorInletTemp=round(i[0],2)
                    if(i[1]==None):
                        Phase1EvaporatorOutletTemp=0
                    else:
                        Phase1EvaporatorOutletTemp=round(i[1],2)
                    if(i[2]==None):
                        Phase1avg_condenserLineInletTemp=0
                    else:
                        Phase1avg_condenserLineInletTemp=round(i[2],2)
                    if(i[3]==None):
                        Phase1avg_condenserLineOutletTemp=0
                    else:
                        Phase1avg_condenserLineOutletTemp=round(i[3],2)
                    if(i[4]==None):
                        Phase1EvaporatorFlowrate=0
                    else:
                        Phase1EvaporatorFlowrate=round(i[4],2)
                    if(i[5]==None):
                        Phase1avg_condenserLineFlowrate=0
                    else:
                        Phase1avg_condenserLineFlowrate=round(i[5],2)
                    thermalinletoutlet_PH1.append({
                        'Phase1EvaporatorInletTemp': Phase1EvaporatorInletTemp,
                        'Phase1EvaporatorOutletTemp':Phase1EvaporatorOutletTemp,
                        'Phase1avg_condenserLineInletTemp':Phase1avg_condenserLineInletTemp,
                        'Phase1avg_condenserLineOutletTemp':Phase1avg_condenserLineOutletTemp,
                        'Phase1EvaporatorFlowrate':Phase1EvaporatorFlowrate,
                        'Phase1avg_condenserLineFlowrate':Phase1avg_condenserLineFlowrate,
                        'Phase1polledTime':Phase1polledTime,
                        'EvapLowerlimit':5,
                        'EvapUpperlimit':18,
                        'CondLowerlimit':20,
                        'CondUpperlimit':40
                     })
       
            return thermalinletoutlet_PH1
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

 

#------------------------- Thermal IN/Out let phase 1 and 2






#------------------------- Chiller Cop 1 to 8 Ph1_Ph2
@app.get('/chillerDashboard/Cop/Phase2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):

    ChillerCop_PH2=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT c1cop,c2cop,c3cop,c4cop,timestamp  FROM meterdata.chillarcop where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Phase2polledTime = str(i[4])[11:16]
        if(i[0]==None):
            Phase2c1cop=0
        else:
            Phase2c1cop=round(i[0],2)

        if(i[1]==None):
            Phase2c2cop=0
        else:
            Phase2c2cop=round(i[1],2)

        if(i[2]==None):
            Phase2c3cop=0
        else:
            Phase2c3cop=round(i[2],2)

        if(i[3]==None):
            Phase2c4cop=0
        else:
            Phase2c4cop=round(i[3],2)
        ChillerCop_PH2.append({
            'Phase2c1cop': Phase2c1cop,
            'Phase2c2cop':Phase2c2cop,
            'Phase2c3cop':Phase2c3cop,
            'Phase2c4cop':Phase2c4cop,
            'Phase2polledTime':Phase2polledTime
         })
       

    return ChillerCop_PH2




@app.get('/chillerDashboard/Cop/Phase1')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):

    ChillerCop_PH2=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT  c5cop,c6cop,c7cop,c8cop,timestamp FROM meterdata.chillarcopphase15678 where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        Phase1polledTime = str(i[4])[11:16]
        if(i[0]==None):
            Phase1c5cop=0
        else:
            Phase1c5cop=round(i[0],2)

        if(i[1]==None):
            Phase1c6cop=0
        else:
            Phase1c6cop=round(i[1],2)

        if(i[2]==None):
            Phase1c7cop=0
        else:
            Phase1c7cop=round(i[2],2)

        if(i[3]==None):
            Phase1c8cop=0
        else:
            Phase1c8cop=round(i[3],2)
        ChillerCop_PH2.append({
            'Phase1c5cop': Phase1c5cop,
            'Phase1c6cop':Phase1c6cop,
            'Phase1c7cop':Phase1c7cop,
            'Phase1c8cop':Phase1c8cop,
            'Phase1polledTime':Phase1polledTime
         })
       

    return ChillerCop_PH2


@app.post('/chillerDashboard/Cop/Phase2/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    ChillerCop_PH2=[]
    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT c1cop,c2cop,c3cop,c4cop,timestamp  FROM meterdata.chillarcopdaywise where date(timestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    Phase2polledTime = str(i[4])[0:10]
                    if(i[0]==None):
                        Phase2c1cop=0
                    else:
                        Phase2c1cop=round(i[0],2)
                    if(i[1]==None):
                        Phase2c2cop=0
                    else:
                        Phase2c2cop=round(i[1],2)
                    if(i[2]==None):
                        Phase2c3cop=0
                    else:
                        Phase2c3cop=round(i[2],2)
                    if(i[3]==None):
                        Phase2c4cop=0
                    else:
                        Phase2c4cop=round(i[3],2)

                    ChillerCop_PH2.append({
                        'Phase2c1cop': Phase2c1cop,
                        'Phase2c2cop':Phase2c2cop,
                        'Phase2c3cop':Phase2c3cop,
                        'Phase2c4cop':Phase2c4cop,
                        'Phase2polledTime':Phase2polledTime
                        })
            return ChillerCop_PH2
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)



@app.post('/chillerDashboard/Cop/Phase1/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    ChillerCop_PH1=[]
    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT  c5cop,c6cop,c7cop,c8cop,timestamp FROM meterdata.chillarcopphase15678 where date(timestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    Phase1polledTime = str(i[4])[0:10]
                    if(i[0]==None):
                        Phase1c5cop=0
                    else:
                        Phase1c5cop=round(i[0],2)
                    if(i[1]==None):
                        Phase1c6cop=0
                    else:
                        Phase1c6cop=round(i[1],2)
                    if(i[2]==None):
                        Phase1c7cop=0
                    else:
                        Phase1c7cop=round(i[2],2)
                    if(i[3]==None):
                        Phase1c8cop=0
                    else:
                        Phase1c8cop=round(i[3],2)

                    ChillerCop_PH1.append({
                        'Phase1c5cop': Phase1c5cop,
                        'Phase1c6cop':Phase1c6cop,
                        'Phase1c7cop':Phase1c7cop,
                        'Phase1c8cop':Phase1c8cop,
                        'Phase1polledTime':Phase1polledTime
                        })
            return ChillerCop_PH1
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)
 

#------------------------- Chiller Cop 1 to 8 Ph1_Ph2



#------------------------- Chiller Total Cooling of the day (TR):
@app.get('/chillerDashboard/TotalCoolingEnergy/Phase2')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):

    TotalCoolingEnergy_PH2=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT totalenergy FROM meterdata.phase2tR where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        if(i[0]==None):
            Phase2TotalCoolingEnergy=0
        else:
            Phase2TotalCoolingEnergy=round(i[0],2)
        TotalCoolingEnergy_PH2.append({'Phase2TotalCoolingEnergy':Phase2TotalCoolingEnergy})
   
    return TotalCoolingEnergy_PH2


@app.get('/chillerDashboard/TotalCoolingEnergy/Phase1')
def peak_demand_date(db: mysql.connector.connect = Depends(get_meterdb)):

    TotalCoolingEnergy_PH1=[]
    try:
        processed_db = get_meterdb()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": f"MySQL connection error: {str(e)}"})
   
    bms_cur = processed_db.cursor()

    bms_cur.execute("SELECT totalenergy FROM meterdata.phase1tR where date(timestamp)=curdate();")
   
    res = bms_cur.fetchall()

    # print(res)

    for i in res:
        if(i[0]==None):
            Phase1TotalCoolingEnergy=0
        else:
            Phase1TotalCoolingEnergy=round(i[0],2)
        TotalCoolingEnergy_PH1.append({'Phase1TotalCoolingEnergy':Phase1TotalCoolingEnergy})
   
    return TotalCoolingEnergy_PH1


@app.post('/chillerDashboard/TotalCoolingEnergy/Phase2/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    TotalCoolingEnergy_PH2=[]

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT totalenergy FROM meterdata.phase2tR where date(timestamp) = '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    if(i[0]==None):
                        Phase2TotalCoolingEnergy=0
                    else:
                        Phase2TotalCoolingEnergy=round(i[0],2)
                    TotalCoolingEnergy_PH2.append({'Phase2TotalCoolingEnergy':Phase2TotalCoolingEnergy})
               
       
            return TotalCoolingEnergy_PH2
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)

       

@app.post('/chillerDashboard/TotalCoolingEnergy/Phase1/Filtered')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_meterdb)):

    TotalCoolingEnergy_PH1=[]

    try:
        value = data.get('date')

        if value and isinstance(value, str):
            with db.cursor() as bmscur:
                bmscur.execute(f"SELECT totalenergy FROM meterdata.phase1tR where date(timestamp)= '{value}'")

                res = bmscur.fetchall()

                for i in res:
                    if(i[0]==None):
                        Phase1TotalCoolingEnergy=0
                    else:
                        Phase1TotalCoolingEnergy=round(i[0],2)
                    TotalCoolingEnergy_PH1.append({'Phase1TotalCoolingEnergy':Phase1TotalCoolingEnergy})
               
       
            return TotalCoolingEnergy_PH1
       
    except mysql.connector.Error as e:
        print(e)
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)



if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5004)


