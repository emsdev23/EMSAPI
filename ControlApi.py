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