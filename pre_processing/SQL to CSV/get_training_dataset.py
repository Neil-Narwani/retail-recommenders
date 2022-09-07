from random import sample
import mysql.connector
import argparse
from fixstring import fix_string
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
parser.add_argument('--server', help='Address of mySQL server (default=localhost)')
parser.add_argument('--username', help='Username to access server (default=sa)')
parser.add_argument('--password', help='Password to access server (required)', required=True)
parser.add_argument('--database', help='Database to access (default=RetailDB)')
parser.add_argument('--samplesize', help='Number of customers to extract from Database (default=5000)')
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

if args.samplesize is None:
    samplesize = 5000
else:
    samplesize = int(args.samplesize)

print('Connect to Database...')
connect_str = 'mysql+mysqlconnector://' + username + ':' + args.password + '@' + server +'/' + database
alchemy_engine = create_engine(connect_str)
read_con = alchemy_engine.connect()

def str2(string):
    if string is not None: 
        return str(string)
    else:
        return ''
        
custfile = f'../anon_customers{samplesize}.csv'
tranfile = f'../transactions{samplesize}.csv'
itemsfile = f'../items{samplesize}.csv'
print(f'Load {samplesize*5} Customers...')
query = f"""select ID,Zip,BirthMonth,TotalSales,TotalSavings,TotalVisits,LastVisit,ReferralCode
    from customer
    where ID not in (select ID from entity_map) and StoreID>=316 limit {samplesize*5}
    """
customers = pd.read_sql(query, read_con)
print(r'Sample and Save 20% random entries')
customers = customers.sample(frac = 0.2)
customers.to_csv(custfile, header=True, index=False)
customers = customers['ID']
print('Save temp customer table to db')
customers.to_sql('tmp_cust', read_con, schema='retaildb', if_exists='replace')

query = r"""
        select transactionentry.ID,CustomerID,Item.ID as ItemID, Item.Description,TransactionTime,Quantity,Price,FullPrice,department.DepartmentName,
            discountcode.code as DiscountCode,returncode.code as ReturnCode
        from transactionentry
        left join item on item.ID = transactionentry.ItemID
        left join department on department.ID = transactionentry.departmentid
        left join reasoncode as discountcode on discountcode.ID = transactionentry.discountid
        left join reasoncode as returncode on returncode.ID = transactionentry.returnid
        where CustomerID in (select ID from tmp_cust)
        """
print(f'Read Transactions for {samplesize} Customers')
transactions = pd.read_sql(query, read_con)
transactions['Description'].map(lambda x: fix_string(x))
transactions['DepartmentName'].map(lambda x: fix_string(x))
print('Save Transactions')
transactions.to_csv(tranfile, header=True, index=False)

print('Read Items and Descriptions')
query = r"""
        select ID,Description
        from item;
        """
items = pd.read_sql(query,read_con)
items['Description'].map(lambda x: fix_string(x))
print('Save Items')
items.to_csv(itemsfile, header=True, index=False)

read_con = mysql.connector.connect(
	host = server,
	database  = database,
	user = username,
	password = args.password
)

with read_con.cursor() as cur:
    print('Cleanup')
    cur.execute(r"""
        drop table tmp_cust;
        """, multi=True)

read_con.close()
