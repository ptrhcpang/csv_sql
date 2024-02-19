#csv_sql2
#Convert csv file to MySQL Database
import pandas as pd
import mysql.connector

#This class TransferObj takes in a csv file and  
#MySQL login details, and allows users either to 
#create a (database and a) table from the csv 
#or insert the csv data into an extant table.
#This is done via reading a csv file in as a 
#pandas dataframe. The only data types permitted 
#to be written into a MySQL table so far are 
#INTs, FLOATs, BOOLs, and VARCHARs (255).
#Standard mysql.connector functions are unimpeded
#and can be accessed directly via 
# >>> TransferObj1.mc.execute(sql_command)
#Future work: 1.allow user to specify n,m in 
#               FLOAT(n,m) type variables
#             2.easy UPDATEs
#             3.MySQL style table output in Python


class TransferObj:
    def __init__(self,csv_filepath,host,user,password):
        self.csv_fp = csv_filepath;
        #self.host = host;
        #self.user = user;
        #self.password = password;
        self.database = None;
        self.mydb = mysql.connector.connect(
            host = host,
            user = user,
            password = password
        )
        self.mc = self.mydb.cursor()
        self.df = pd.read_csv(csv_filepath)
        self.rown,self.coln = self.df.shape[:]
        self.INTind = []  #column index of integers
        self.FLOATind = [] #column index of floats
        self.BOOLind = [] #column index of booleans
        self.Vals = [] #list of values to be written
    
    
    
    #specify database
    def useDb(self,Db):
        self.mc.execute('SHOW DATABASES;')
        AvailableDBs = []
        for x in self.showmc():
            AvailableDBs.append(x[0]) #SHOW DATABASES outputs a list of tuples, the 0th element of each tuple is the database name
        if Db not in AvailableDBs:
            print('database not available or unspecified --- using database '+ str(self.database))
            return 0
        #only modify self.database once Db is actually in the list of usable databases
        self.database = Db 
        self.mc.execute('USE '+ Db)
        return 'using database ' + Db
    
    
    #takes in column numbers
    def __indices__(self,ind):
        ind2 = [int(abs(s)) for s in ind if str(s).isdigit()] #prune all non-integers
        ind2 = list(set(ind2)) #sorts the index list
        length = len(ind2)
        for x in range(length):
            if ind[x] >= self.coln:
                print('index exceeds range')
                return ind2[:x]
        return ind2
    
    
    #the next three functions specify
    #which columns are of INT, FLOAT, or BOOLEAN type
    def toINT(self,ind):
        self.INTind = self.__indices__(ind) #INTind are the column indices for INT variables
    
    def toFLOAT(self,ind):
        self.FLOATind = self.__indices__(ind) #FLOATind are the column indices for FLOAT variables
    
    def toBOOL(self,ind):
        self.BOOLind = self.__indices__(ind) #BOOLind are the column indices for BOOLEAN variables
    
    #unless column indices have been reserved 
    #for numeric or boolean values, they are 
    #treated as VARCHAR(255) variables
    def __toSTR__(self):
        #if FLOATind and INTind overlap, 
        #suppose the column is assumed 
        #to be for FLOAT variables.
        self.INTind = [x for x in self.INTind if x not in self.FLOATind]
        #if BOOLind overlaps with FLOATind 
        #or INTind, the column is assumed not to be BOOLEAN.
        self.BOOLind = [x for x in self.BOOLind if x not in self.INTind and x not in self.FLOATind] 
        #every non-numeric non-boolean 
        #column is taken to be a varchar(255) column.
        self.STRind = [x for x in list(range(self.coln)) if (x not in self.FLOATind and x not in self.INTind and x not in self.BOOLind)]
    
    #cast csv entries to correct types,     
    def __toVALUES__(self):
        #list of column headings
        columns = list(self.df.columns)
        #builds MySQL values tuple, converts 'nan' to None
        #other quantities' type conversions to be customised
        self.__toSTR__() #finalise which are ints, floats, and booleans
        #casts data into types
        for x in range(self.rown):
            a = [];
            for y in range(self.coln):
                t = str(self.df.loc[x][y]);
                if t == 'nan':
                    a.append(None)
                    continue
                if y in self.FLOATind:
                    a.append(float(self.df.loc[x][y]))
                    continue
                if y in self.INTind:
                    a.append(int(float(self.df.loc[x][y])))
                    continue
                if y in self.BOOLind:
                    if self.df.loc[x][y] in ['False','FALSE','0']:
                        self.df.loc[x][y] = False
                    a.append(bool(self.df.loc[x][y]))
                    continue                
                a.append(t)
            self.Vals.append(tuple(a))
    
    def showmc(self):
        ShowMC = []
        for x in self.mc:
            ShowMC.append(x)
        return ShowMC
    
    def close(self):
        self.mc.close()
        return True
    
    #create a new table with the csv column headings if needed
    def createtable(self,table):
        #make sure we are in a certain database first
        if self.database == None:
            print("no database selected")
            return 0
        sql = 'CREATE TABLE '+ table + '('
        columns = list(self.df.columns)
        for x in range(self.coln):
            if x in self.FLOATind:
                sql = sql + columns[x] + ' FLOAT, '
                continue
            if x in self.INTind:
                sql = sql + columns[x] + ' INT, '
                continue
            if x in self.BOOLind:
                sql = sql + columns[x] + ' BOOLEAN, '
                continue                
            sql = sql + columns[x] + ' VARCHAR(255), '
        sql = sql[:-2] + ')'
        self.mc.execute(sql)
        print('table ' + table + ' created in ' + self.database)
        self.mc.execute('SHOW COLUMNS FROM '+ table)
        self.showmc()
        
    #insert csv file into an existing table
    def insertinto(self,table):
        self.mc.execute('SHOW TABLES;')
        AvailableTabs = []
        for x in self.showmc():
            AvailableTabs.append(x[0])
        if table not in AvailableTabs:
            print('Table not available.')
            return 0
        sql = 'INSERT INTO ' + table + ' ('
        sqlv = ') VALUES('
        coln2 = 0
        #finds the table columns
        self.mc.execute('SHOW COLUMNS FROM '+ table+';')
        #composes the sql string to be executed
        for x in self.mc:
            sql = sql + x[0] + ','
            sqlv = sqlv + '%s,'
            coln2 = coln2 + 1
        sql = sql[:-1]  + sqlv[:-1] + ')'
        #if the columns from the table matches 
        #the number of columns in the csv file/dataframe, 
        #write values into database
        if coln2 == self.coln:
            self.__toVALUES__()
            self.mc.executemany(sql,self.Vals)
            a = self.showmc()
            self.mydb.commit()
        return 'csv inserted'

###
