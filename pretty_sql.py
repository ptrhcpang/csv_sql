#This is a small script that helps 
#mysql connector to display tables beautifully.
#The display function is displayTable()

import mysql.connector

#establish mysql connection
mcon = mysql.connector.connect(
            host = host,
            user = user,
            password = password
            )

#clears out connector
def showmc():
	ShowMC = []
	for x in self.mc:
		ShowMC.append(x)
	return ShowMC

####
#functions for understanding the size of table to be displayed
####
def findColumns(mcon,table):
	showmc()
	C = []
	mcon.execute('SHOW COLUMNS FROM '+table+';')
	
	for x in mcon:
		C.append(x[0])
	
	return C

def findLength(mcon,table,column):
	showmc()
	mcon.execute('SELECT '+column+ ' FROM '+table+';')
	a = 0
	for x in mcon:
		if len(str(x[0])) > a:
			a = len(str(x[0]))
	
	if len(column) > a:
		a = len(column)
	return a

def widthDict(mcon,table):
	showmc()
	C = findColumns(mcon,table)
	A = []
	for column in C:
		A.append((column,findLength(mcon,table,column) + 2))
	return A

####
#functions for column and row dividers
#####

def emptyStr(n):
	s = ''
	for x in range(n):
		s = s + ' '
	return s

def dashStr(n):
	s = ''
	for x in range(n):
		s = s + '-'
	return s
	


#function for displaying the table
#    this is the function that is actually called
def displayTable(mcon,table):
	R = []
	for x in mcon:
		R.append(x)
	
	A = widthDict(mcon,table)
	headline = '|'
	dashline = '+'
	
	for x in A:
		gap = x[1]-len(x[0])
		headline = headline + ' ' + x[0] + emptyStr(gap) + '|'
		dashline = dashline + dashStr(x[1] + 1) + '+'

	print(dashline)
	print(headline)
	print(dashline)
	newline = '|'

	for x in R:
		for n in range(len(x)):
			gap = A[n][1] - len(str(x[n]))
			newline = newline + ' ' + str(x[n]) + emptyStr(gap) + '|'
		#newline = newline + '\n'
		print(newline)
		newline = '|'
	print(dashline)
	    

