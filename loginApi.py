from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector
from fastapi.responses import JSONResponse
import json
from datetime import datetime

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

def get_awsdb():
    db = mysql.connector.connect(
        host=data['awsDB']['host'],
        user=data['awsDB']['user'],
        password=data['awsDB']['password'],
        database='EMS',
        port=data['awsDB']['port']
    )
    return db

@app.post('/login')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):

    try:
        user = data.get('username')
        password = data.get('password')

        if user and password and isinstance(user, str):
            print(user,password)
            with db.cursor() as awscur:
                awscur.execute(f"select username,password from EMS.AdminEMS where username = '{user}'")

                res = awscur.fetchall()

                if len(res) > 0:
                    if password == res[0][1]:
                        return {"message":"login sucessful","team":"admin","username":user}
                    else:
                        return {"message":"incorrect password"}
                else:
                    awscur.execute(f"select username,password,team from EMS.UserEMS where username = '{user}'")

                    res1 = awscur.fetchall()

                    if len(res1) > 0:
                        if password == res1[0][1]:
                            return {"message":"login sucessful","team":res1[0][2],"username":user}
                        else:
                            return {"message":"incorrect password"}
                    else:
                        return {"message":"User not found"}
    
    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)
    

@app.post('/registerUser')
def peak_demand_date(data: dict, db: mysql.connector.connect = Depends(get_awsdb)):
    
    try:
        user = data.get('username')
        password = data.get('password')
        email = data.get('email')
        team = data.get('team')
        curdate = datetime.now()

        if user and password and email and team:
            sql = "INSERT INTO EMS.UserEMS(username,email,password,team,createdTime) values(%s,%s,%s,%s,%s)"
            val = (user,email,password,team,curdate)
            awsdb = mysql.connector.connect(
                    host="3.111.70.53",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
            awscur = awsdb.cursor()
            
            try:
                awscur.execute(sql,val)
                awsdb.commit() 

                return {"message":'User registered sucessfully'}
            except mysql.connector.errors.IntegrityError as ex:
                ex = str(ex)
                if 'PRIMARY' in ex:
                    return {"message":"User already exists"}
                elif 'email' in ex:
                    return {"message":"User already registered with this email"}

    except mysql.connector.Error as e:
        return JSONResponse(content={"error": "MySQL connection error"}, status_code=500)
    

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5008)