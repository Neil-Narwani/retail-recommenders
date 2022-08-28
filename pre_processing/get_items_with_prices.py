import mysql.connector
import datetime
from decimal import Decimal
from fixstring import fix_string

server = 'localhost'
database = 'RetailDB'
username = 'sa'
password = 'rmssa512'

read_con = mysql.connector.connect(
	host = server,
	database  = database,
	user = username,
	password = password
)

itemsfile = '../items.csv'

with read_con.cursor(dictionary=True) as cur:
    print('Step 6: Items') 
    outfile = open(itemsfile, 'w', encoding='utf-8')
    outfile.write('ID,Description,FullPrice\n')
    cur.execute(r"""
        select ID,Description,FullPrice
        from item;
        """)
    for row in cur:
        outfile.write(str(row['ID'])+','+fix_string(row['Description'])+','+str(row['FullPrice'])+'\n') 
    outfile.close()