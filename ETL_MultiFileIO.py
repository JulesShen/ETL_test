import os

import pandas as pd

import mysql.connector as ms

class MultiFileETL(object):

    def __init__(self, folder):
        self.path = os.getcwd() + "\\" + folder

    def readcsv(self, filename):
        ffile = self.path + "\\" + filename + '.csv'
        self.data = pd.read_csv(ffile)
        cols = self.data.columns
        nodupcol = []
        nullcol = []
        for i in cols:
            if not self.data.duplicated([i]).max():
                nodupcol.append(i)
            if self.data.isnull().any()[i]:
                nullcol.append(i)

        print(f"{filename} has been loaded in to CSV format")
        print(f"Number of records: {len(self.data)}")
        print(f"Number of columns: {len(self.data.columns)}")
        print(f"Columns without duplicated records: {nodupcol}")
        print(f"Columns with missing values: {nullcol}")

    def mysql_create_table(self, user, password, host,schema,table_name):
        self.user = user
        self.password = password
        self.host = host
        self.table_name = table_name
        self.schema = schema

        cnx = ms.connect(user = str(self.user), password = str(self.password),
                         host = str(self.host), database = str(self.schema))
        assert cnx

        cursor = cnx.cursor()

        # Check table
        check_table = (f"select 1 from {self.table_name} LIMIT 1")

        # Create table
        create_table = (
                       f"""create table {self.table_name}
                       (
                       PassengerID int(32) NOT null, 
                       Survived  int(8) not null,
                       Pclass int(8) not null,
                       Name varchar(30),
                       Sex varchar (8),
                       Age float,
                       SibSp int(8),
                       Parch int(8),
                       Ticket varchar(16),
                       Fare float,
                       Cabin varchar(32),
                       Embarked varchar(32),
                       CONSTRAINT PID primary key (PassengerID)
                       ) ENGINE = InnoDB """
                       )
        try:
            cursor.execute(check_table)
            result = (cursor.fetchall()[0][0])
            print("Number of min-exist, %s, Table existed" % result)
        except:
            cursor.execute(create_table)
            print("New table, %s, is created" % table_name)
































