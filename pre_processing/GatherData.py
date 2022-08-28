import pymssql
import mysql.connector
import re
from ftfy import fix_text
import argparse

parser = argparse.ArgumentParser(description='Get clean customer data from MsSQL db for all stores')
parser.add_argument('--server', help='Address of mySQL server (default=localhost)')
parser.add_argument('--username', help='Username to access server (default=sa)')
parser.add_argument('--password', help='Password to access server (required)', required=True)
parser.add_argument('--test', help='Enable Test mode to limit transactions')
args = parser.parse_args()

if args.username is None:
	username = 'sa'
else:
	username = args.username

if args.server is None:
	server = 'localhost'
else:
	server = args.server

if args.test is None:
    stores = [ '511', '512', '582', '310', '316', '321', '501', '504', '519', '550', '566' ]
    limit = -1
else:
    stores = [ '511' ]
    limit = int(args.test)

output = 'customers.csv'
outfile = open(output, 'w', encoding='utf-8')
dupsrx = re.compile(r'(.)\1{5,}') # Search for any thing that has more than 5 repeated characters
chars_to_remove = [")","(","|","[","]","{","}", ",", "'", '\"']
badcharsrx = re.compile('[' + re.escape(''.join(chars_to_remove)) + ']') # Replace quotes and commas.
badaddress = re.compile(r'bad address', flags=re.IGNORECASE)
unabletoforward = re.compile(r'unable to forward', flags=re.IGNORECASE)


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
    if string.isspace() or dupsrx.search(string) != None or badaddress.search(string) != None or unabletoforward.search(string) != None :
        string = ''

    return string

outfile.write('ID,StoreID,CompanyName,FirstName,LastName,Address,Address2,City,State,Zip,EmailAddress,PhoneNumber,BirthMonth,ReferralSource,TotalSales,LastVisit,TotalVisits,TotalSavings\n')
for stid in stores:
    print('Working on Store: ' + stid)
    database = 's' + stid
    cnxn = pymssql.connect(server, username, args.password, database)
    cursor = cnxn.cursor()
    if limit < 0:
        prequery = 'SELECT'
    else:
        prequery = 'SELECT TOP {limit} '.format(limit=limit)
    query = prequery + r"""
      [ID] as ID,
      '{storeid}' as StoreID,
      [Company] as CompanyName,
      [FirstName] as FirstName,
      [LastName] as LastName,
      [Address] as Address,
      [Address2] as Address2,
      [City] as City,    
      [State] as State,
      [Zip] as Zip,      
      [EmailAddress] as EmailAddress,
      [PhoneNumber] as PhoneNumber,      
      [CustomNumber1] as BirthMonth,
      [CustomText1] as ReferralSource,
      [TotalSales] as TotalSales,
      [LastVisit] as LastVisit,
      [TotalVisits] as TotalVisits,
      [TotalSavings] as TotalSavings
    FROM Customer
    where Customer.AccountNumber <> 'CASH' and 
        TotalVisits >= 3 and
        Customer.ID in (select distinct CustomerID from [Transaction])""".format(storeid=stid)
    cursor.execute(query)
    # iterate through each row and cleanup the strings
    # if a row has > 4 strikes (null strings) drop it.
    for row in cursor:
        ndx = 0
        strikes = 0
        outrow = ''
        for col in row:
            clean = fixstring(col)            
            if clean == '':
                strikes = strikes + 1
            outrow = outrow + clean
            ndx = ndx + 1
            if ndx < len(row):
                outrow = outrow + ','
                
        if strikes <= 4:
            outfile.write(outrow)
            outfile.write('\n')
    print('Done.')

outfile.close()
            
            