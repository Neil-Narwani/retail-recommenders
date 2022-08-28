from random import sample
import mysql.connector
import datetime
from decimal import Decimal
import argparse
from fixstring import fix_string,str2
import re

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

def add_values_in_dict(dict, key, value):
    ''' Append multiple values to a key in 
        the given dictionary '''
    if key not in dict:
        dict[key] = list()
    dict[key].append(value)
    return dict
        
entity_map = {}
custfile = '../anon_customers.csv'
tranfile = '../transactions.csv'
itemsfile = '../items.csv'
reasoncodefile = '../reasoncodes.csv'
departmentsfile = '../departments.csv'

print('Initialize... Reading duplicate customers')
with read_con.cursor() as cur:
    cur.execute(r"""select id, canon_id from entity_map;""")
    for row in cur:
        entity_map = add_values_in_dict(entity_map, row[1], row[0] )

with read_con.cursor(dictionary=True) as cur:
    print("Step 1")
    outfile = open(custfile, 'w', encoding='utf-8')
    outfile.write('ID,Zip,BirthMonth,TotalSales,TotalSavings,TotalVisits,LastVisit,ReferralCode\n')
    
    for id in entity_map.keys():
        dupes = ",".join(map(str, entity_map[id]))
        cur.execute(r"""
            select ID,Zip,BirthMonth,TotalSales,TotalSavings,TotalVisits,LastVisit,ReferralCode
            from customer
            where ID in ( %s )
        """ % dupes)
        totalsavings = Decimal(0)
        totalsales = Decimal(0)
        totalvisits = Decimal(0)
        Zip = ''
        BirthMonth = ''
        ReferralCode = ''
        LastVisit = datetime.datetime(2000, 1, 1, 0, 0, 0)
        for row in cur:
            totalsales = totalsales + row['TotalSales']
            totalsavings = totalsavings + row['TotalSavings']
            totalvisits = totalvisits + row['TotalVisits']
            if row['Zip'] is not None:
                Zip = row['Zip']
            if row['ReferralCode'] != '':
                ReferralCode = row['ReferralCode']
            if row['LastVisit'] is not None and row['LastVisit'] > LastVisit:
                LastVisit = row['LastVisit']
        outfile.write(str(id)+','+Zip+','+BirthMonth+','+str(totalsales)+','+str(totalsavings)+','+
            str(totalvisits)+','+str(LastVisit)+','+ReferralCode+'\n')
    
    print('Complete.')
    outfile.close()

with read_con.cursor(dictionary=True) as cur:
    print('Step 2.')
    outfile = open(custfile, 'a', encoding='utf-8')
    cur.execute(r"""
        select customer.ID,Zip,BirthMonth,TotalSales,TotalSavings,TotalVisits,LastVisit,ReferralCode
        from customer
        where ID not in (select id from entity_map)
        """)
    for row in cur:
        outfile.write(str2(row['ID'])+','+str2(row['Zip'])+','+str2(row['TotalSales'])+','+
            str2(row['TotalSavings'])+','+str2(row['TotalVisits'])+','+str2(row['LastVisit'])+','+
            str2(row['ReferralCode'])+'\n')
    print('Complete.')
    outfile.close()

with read_con.cursor(dictionary=True) as cur:
    print('Step 3. (It will be a minute)')
    outfile = open(tranfile, 'w', encoding='utf-8')
    outfile.write('ID,CustomerID,Description,TransactionTime,Quantity,Price,FullPrice,DepartmentName,DiscountCode,ReturnCode\n')
    for id in entity_map.keys():
        dupes = ",".join(map(str, entity_map[id]))
        cur.execute("""
            select transactionentry.ID,CustomerID,Item.Description,TransactionTime,Quantity,Price,FullPrice,department.DepartmentName,
                discountcode.code as DiscountCode,returncode.code as ReturnCode
            from transactionentry
            left join item on item.ID = transactionentry.ItemID
            left join department on department.ID = transactionentry.departmentid
            left join reasoncode as discountcode on discountcode.ID = transactionentry.discountid
            left join reasoncode as returncode on returncode.ID = transactionentry.returnid
            where CustomerID in ( %s );
        """ % dupes)
        for row in cur:
            outfile.write(str(row['ID'])+','+str(id)+ ','+fix_string(row['Description'])+','+str2(row['TransactionTime'])+','+
                str(row['Quantity'])+','+str(row['Price'])+','+str(row['FullPrice'])+','+fix_string(row['DepartmentName'])+','+
                str(row['DiscountCode'])+','+str(row['ReturnCode'])+'\n')
    print('Complete.')
    outfile.close()



with read_con.cursor(dictionary=True) as cur:
    print('Step 4. It will be about another minute')
    outfile = open(tranfile, 'a', encoding='utf-8')
    cur.execute(r"""
        select transactionentry.ID,CustomerID,Item.Description,TransactionTime,Quantity,Price,FullPrice,department.DepartmentName,
            discountcode.code as DiscountCode,returncode.code as ReturnCode
        from transactionentry
        left join item on item.ID = transactionentry.ItemID
        left join department on department.ID = transactionentry.departmentid
        left join reasoncode as discountcode on discountcode.ID = transactionentry.discountid
        left join reasoncode as returncode on returncode.ID = transactionentry.returnid
        where CustomerID not in (select id from entity_map);
        """)
    for row in cur:
            outfile.write(str(row['ID'])+','+str(id)+ ','+fix_string(row['Description'])+','+str2(row['TransactionTime'])+','+
                str(row['Quantity'])+','+str(row['Price'])+','+str(row['FullPrice'])+','+fix_string(row['DepartmentName'])+','+
                str(row['DiscountCode'])+','+str(row['ReturnCode'])+'\n')
    print('Complete.')
    outfile.close()

with read_con.cursor(dictionary=True) as cur:
    print('Step 5: ReasonCodes') 
    outfile = open(reasoncodefile, 'w', encoding='utf-8')
    outfile.write('ID,Description\n')
    cur.execute(r"""
        select ID, Description
        from reasoncode""")
    for row in cur:
        outfile.write(str(row['ID'])+','+fix_string(row['Description'])+'\n') 
    outfile.close()

with read_con.cursor(dictionary=True) as cur:
    print('Step 6: Items') 
    outfile = open(itemsfile, 'w', encoding='utf-8')
    outfile.write('ID,Description,FullPrice\n')
    cur.execute(r"""
        select ID,Description,FullPrice
        from item;
        """)
    for row in cur:
        outfile.write(str(row['ID'])+','+fix_string(row['Description'])+','+row['FullPrice']+'\n') 
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
        outfile.write(str(row['ID'])+','+fix_string(row['DepartmentName'])+'\n') 
    outfile.close()
read_con.close()
