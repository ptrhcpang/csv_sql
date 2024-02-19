#Date: 19/Feb/2024

This class TransferObj takes in a csv file and  
MySQL login details, and allows users either to  
create a (database and a) table from the csv  
or insert the csv data into an extant table. 

Standard mysql.connector functions are unimpeded  
and can be accessed directly via 

>>>TransferObj1.mc.execute(sql_command)

the TransferObj class consists of the following functions: 

###

\__init\__(self,csv_filepath,host,user,password)

requires :the filepath of the csv file to be transferred  
requires: the host, user, and password for MySQL access  
creates the variables:  
mydb : the connection handle  
mc: MySQL cursor object  
df: pandas dataframe from the csv file  
rown, coln: the number of rows and number of columns of df   
empty lists FLOATind, INTind, BOOLind: to hold   
column indices of columns that should contain  
floats, integers, boolean values, respecitvely  
empty list Vals: to hold row values that will   
be inserted into the SQL table  

###

useDb(self,Db) 

requires a database name  
checks that this database exists   
changes to this database if exists == True.  

###

\__indices\__(self,ind)

requires a list of column indices of df.  
used for passing lists of column indices  
into FLOATind, INTind, BOOLind variables.  
checks that the lists consist of integers  
only and are not out of range of the number   
of columns that exist in the dataframe df.  

###

toINT(self,ind)

uses \__indices\__() to update the INTind variable.  

###

toFLOAT(self,ind)

uses \__indices\__() to update the FLOATind variable.  

###

toBOOL(self,ind)

uses \__indices\__() to update the BOOLind variable.  

###

\__toSTR\__(self)

this function harmonises all the index lists  
so that there are no overlaps.   
if an index is in both FLOATind and INTind,   
it is removed from INTind.  
if an index is in BOOLind and another index list,  
it is removed from BOOOLind.  

###

\__toVALUES\__(self)

this function uses the harmonised INTind, FLOATind,   
and BOOLind index lists and casts the entries   
of df into their correct types to prevent exceptions   
from arising in inserting the df into a MySQL table.  
any column index of df not in INTind, FLOATind,   
or BOOLind is automatically assumed to be of   
VARCHAR(255) type.  
'nan's are replaced by None

###

createtable(self,table)  

creates a MySQL table in the database with name table.  
aborts if no database is selected.  

###

insertinto(self,table)  

inserts the dataframe df into the MySQL table table.  
aborts if no table with name table is available.  

###

showmc(self)

this is used internally to flush out the cursor mc  
it can also be called publically and works   
in the same way as asking TransferObj1.mc   
to put all its output into a list.  

###

close(self)

closes the connection.  



