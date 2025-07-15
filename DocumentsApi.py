import boto3
import io
from flask import Flask, jsonify, request, send_file
from botocore.exceptions import NoCredentialsError
from werkzeug.utils import secure_filename
from flask_cors import CORS
import mysql.connector
from datetime import datetime

app = Flask(__name__)
CORS(app)

AWS_ACCESS_KEY_ID = 'AKIA24JMII2RM7XU2JUA'
AWS_SECRET_ACCESS_KEY = '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'

file_path = "/home/tenet/ems/Creds/db_creds.json"

with open(file_path, 'r') as file:
    data = json.load(file)


def uploads3_file(file, acl="public-read"):

    s3 = boto3.client(
        service_name='s3',
        region_name='ap-south-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    
    filename = secure_filename(file.filename)
    bucket = 'emstestbuck1'

    try:
        s3.upload_fileobj(
            file,
            bucket,
            file.filename,
            ExtraArgs={
                "ContentType": file.content_type
            }
        )
    
    except Exception as e:
        # This is a catch all exception, edit this part to fit your needs.
        print("Something Happened: ", e)
        return e

@app.route('/getdocs', methods=['GET'])
def get_docs():
    try:
        awsdb = mysql.connector.connect(
                        host=data['emsDB']['host'],
                        user=data['emsDB']['user'],
                        password=data['emsDB']['password'],
                        database='EMS',
                        port=data['emsDB']['port']
                    )
            
        awscur = awsdb.cursor()
    except Exception as ex:
        print(ex)

    try: 
        awscur.execute("select recordId, uploadTime, fileName, documentType, systemName, documentName from EMS.documents;")
        docs = awscur.fetchall()
    except Exception as ex:
        docs = None
        print(ex)
    
    if docs != None:
        print(docs)
        return docs
    else:
        print(docs)
        return docs


@app.route('/sample', methods=['POST'])
def sample(): 
    obj = request.json['pin']
    print(obj)

    return obj

@app.route('/uploadbill', methods=['POST'])
def upload_bill(): 
    try:
        
        if 'file' not in request.files:
            return jsonify({"error": "No file part"})

        file = request.files['file']

        if file.filename == '':
            return jsonify({"error": "No selected file"})

        object_name = file.filename
        document_type = request.form.get('fileType')
        systemName = request.form.get('systemName')
        documentName = request.form.get('DocumentName')
        print(object_name,document_type,systemName,documentName)

        cur = datetime.now()

        # s3.upload_file(Filename=file.filename, Bucket=bucket, Key=object_name)
        try:
            awsdb = mysql.connector.connect(
                        host=data['emsDB']['host'],
                        user=data['emsDB']['user'],
                        password=data['emsDB']['password'],
                        database='EMS',
                        port=data['emsDB']['port']
                    )
            
            awscur = awsdb.cursor()
        except Exception as ex:
            print(ex)
        
        val = (cur,document_type,object_name,systemName,documentName)
        sql = "INSERT INTO EMS.documents(uploadTime,documentType,fileName,systemName,documentName) values(%s,%s,%s,%s,%s)"

        try:
            awscur.execute(sql,val)
            awsdb.commit()
            print(f"{object_name} uploaded at {cur}")
            uploads3_file(file)
            awscur.close()
            return jsonify({"message": f"{object_name} uploaded successfully"})
        
        except mysql.connector.IntegrityError as ex:
            awscur.close()
            return jsonify({"message":f"{object_name} already exists"})
        
    except Exception as e:
        return jsonify({"error": str(e)})
    

@app.route('/deletefile/<file_key>', methods=['DELETE'])
def delete_file(file_key):
    s3 = boto3.client(
        service_name='s3',
        region_name='ap-south-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    
    # json_data = request.get_json()

    try:
        # file_name = json_data['filename']
        
        # if not file_name:
            # return jsonify({"error": "Filename not provided"})

        # Specify the bucket and key (filename) of the file to delete
        bucket = 'emstestbuck1'

        cur = datetime.now()

        # s3.upload_file(Filename=file.filename, Bucket=bucket, Key=object_name)
        try:
            awsdb = mysql.connector.connect(
                        host=data['emsDB']['host'],
                        user=data['emsDB']['user'],
                        password=data['emsDB']['password'],
                        database='EMS',
                        port=data['emsDB']['port']
                    )
            
            awscur = awsdb.cursor()
        except Exception as ex:
            print(ex)

        print(file_key)
        sql = f"delete from EMS.documents where fileName = '{file_key}'"

        try:
            awscur.execute(sql)
            awsdb.commit()
            print(f"{file_key} deleted at {cur}")
            s3.delete_object(Bucket=bucket, Key=file_key)
            awscur.close()
            return jsonify({"message": [file_key,f"{file_key} deleted successfully"]})
        except Exception as ex:
            awscur.close()
            print(ex)
            return jsonify({"message": [file_key,f"{file_key} not deleted successfully"]})

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/downloadfile/<file_key>', methods=['GET'])
def get_presigned_url(file_key):
    s3 = boto3.client(
        service_name='s3',
        region_name='ap-south-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

    bucket_name = 'emstestbuck1'
    

    try:
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket_name, 'Key': file_key},
            ExpiresIn=3600  # URL expiration time in seconds
        )
        return jsonify({'presigned_url': url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5007)
