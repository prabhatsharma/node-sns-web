import pyodbc
import boto3
import json
import socket

server = 'localhost'
database = 'epglobal'
username = 'node1'
password = 'hello#123'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' +
                      server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD=' + password)
cursor = cnxn.cursor()

sqs = boto3.resource('sqs', region_name='us-west-2')
queue = sqs.get_queue_by_name(QueueName='sql2')

print("Hello")
def processInsert(data):
    try:
        tsql = "INSERT INTO customer (id, cname) VALUES (?,?);"
        with cursor.execute(tsql, data['uid'], data['customername']):
            print('Successfuly Inserted!')
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate) 

def processUpdate(data):
    sql = ""
    print("updating")


def processDelete(data):
    sql = ""
    print("deleting")

def originInsert(data):
    try:
        tsql = "INSERT INTO customerorigin (id, origin) VALUES (?,?);"
        with cursor.execute(tsql, data['uid'], data['origin']):
            print('Origin Inserted!')
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)


hostname = socket.gethostname()
print('hostname: ', hostname)

while True:
    messages = queue.receive_messages(
        WaitTimeSeconds=20, MaxNumberOfMessages=1)
    for message in messages:
        data = json.loads(message.body)
        body = json.loads(data['Message'])
        # print(messages, type(messages))
        print('1: ', body)
        if body['origin'] == hostname:
            message.delete()
        else:
            if body['dmlType'] == 'insert':
                processInsert(body)
                originInsert(body)
                message.delete()
            elif body['dmlType'] == 'update':
                processUpdate(body)
                message.delete()
            elif body['dmlType'] == 'delete':
                processDelete(body)
                message.delete()
        # Let the queue know that the message is processed


# #Update Query
# print ('Updating Location for Nikita')
# tsql = "UPDATE Employees SET Location = ? WHERE Name = ?"
# with cursor.execute(tsql,'Sweden','Nikita'):
#     print ('Successfuly Updated!')


# #Delete Query
# print ('Deleting user Jared')
# tsql = "DELETE FROM Employees WHERE Name = ?"
# with cursor.execute(tsql,'Jared'):
#     print ('Successfuly Deleted!')


# Select Query
# print ('Reading data from table')
# tsql = "SELECT * from customer;"
# with cursor.execute(tsql):
#     row = cursor.fetchone()
#     while row:
#         print (str(row[0]) + " " + str(row[1]))
#         row = cursor.fetchone()
