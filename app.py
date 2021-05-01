from flask import Flask,render_template,request
import os
import csv
import mysql.connector  
import pandas as pd
import numpy
app = Flask(__name__)

@app.route('/',methods=['GET','POST'])
def index():
    if request.method == 'POST':
        file = request.files['csvfile']
        if not os.path.isdir('static'):
            os.mkdir('static')
        filepath=os.path.join('static',file.filename)
        file.save(filepath)
        return 'The File name  of the uploaded file is {}'.format(file.filename)
    return render_template('index.html')


df_eff_memory = pd.read_csv('./static/aaa.csv',sep=',',dtype={'Month':'string','Average':'string','Mark':'string'})
df_eff_memory.memory_usage(index=False,deep=True)

mydb=mysql.connector.connect(host='localhost',user='root',password='saro2001@1.',database='movie')
cur=mydb.cursor()
measurement_insert_query ="""insert into aaa (Month,Average,Mark) values(%s,%s,%s)"""
data = list(zip(df_eff_memory['Month'],df_eff_memory['Average'],df_eff_memory['Mark']))

for chunk in pd.read_csv('./static/aaa.csv',sep=',',dtype={'Month':'string','Average':'string','Mark':'string'},chunksize=10000):
    
    # zip the data
    data = list(zip(chunk['Month'].to_numpy(),chunk['Average'].to_numpy(),chunk['Mark'].to_numpy()))
    
    # insert data into db
    try:
        cur.executemany(measurement_insert_query,data)
        mydb.commit()
        print('success')
    
    except Exception as e:
        mydb.close()
        print(e)

if __name__ == '__main__':
 app.run(debug=True)
