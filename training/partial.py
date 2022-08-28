
import mysql.connector
import datetime
from decimal import Decimal
import argparse
from ftfy import fix_text
import re

chars_to_remove = [")","(","|","[","]","{","}", ",", "'", '\"']
badcharsrx = re.compile('[' + re.escape(''.join(chars_to_remove)) + ']') # Replace quotes and commas.

def fixstring(string):
    string = str(string)
    string = fix_text(string) # fix text
    string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
    string = string.lower()
    string = badcharsrx.sub('', string)
    string = string.replace('&', 'and')
    string = string.replace(',', ' ')
    string = string.lower() # normalise case
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    return string

parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
parser.add_argument('--server', help='Address of mySQL server (default=localhost)')
parser.add_argument('--username', help='Username to access server (default=sa)')
parser.add_argument('--password', help='Password to access server (required)', required=True)
parser.add_argument('--database', help='Database to access (default=RetailDB)')
args = parser.parse_args()

if args.database is None:
	database = 'RetailDB'
else:
	database = args.database
	
if args.username is None:
	username = 'sa'
else:
	username = args.username

if args.server is None:
	server = 'localhost'
else:
	server = args.server

read_con = mysql.connector.connect(
	host = server,
	database  = database,
	user = username,
	password = args.password
)

def str2(string):
    if string is not None: 
        return str(string)
    else:
        return ''
reasoncodefile = 'reasoncodes.csv'
itemsfile = 'items.csv'
departmentsfile = 'departments.csv'


with read_con.cursor(dictionary=True) as cur:
    print('Step 5: ReasonCodes') 
    outfile = open(reasoncodefile, 'w', encoding='utf-8')
    outfile.write('ID,Description\n')
    cur.execute(r"""
        select ID, Description
        from reasoncode""")
    for row in cur:
        outfile.write(str(row['ID'])+','+fixstring(row['Description'])+'\n') 
    outfile.close()

with read_con.cursor(dictionary=True) as cur:
    print('Step 6: Items') 
    outfile = open(itemsfile, 'w', encoding='utf-8')
    outfile.write('ID,Description\n')
    cur.execute(r"""
        select ID,Description
        from item;
        """)
    for row in cur:
        outfile.write(str(row['ID'])+','+fixstring(row['Description'])+'\n') 
    outfile.close()

with read_con.cursor(dictionary=True) as cur:
    print('Step 7: Departments (final step)') 
    outfile = open(departmentsfile, 'w', encoding='utf-8')
    outfile.write('ID,DepartmentName\n')
    cur.execute(r"""
        select ID,DepartmentName
        from department;
        """)
    for row in cur:
        outfile.write(str(row['ID'])+','+fixstring(row['DepartmentName'])+'\n') 
    outfile.close()
read_con.close()