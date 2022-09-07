use s511
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=511
go
delete from TempTxn
go

use s512
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=512
go
delete from TempTxn
go

use s582
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=582
go
delete from TempTxn
go

use s310
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=310
go
delete from TempTxn
go

use s316
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=316
go
delete from TempTxn
go

use s321
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=321
go
delete from TempTxn
go

use s501
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=501
go
delete from TempTxn
go

use s504
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=504
go
delete from TempTxn
go

use s519
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=519
go
delete from TempTxn
go

use s550
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=550
go
delete from TempTxn
go

use s566
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.Name, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s511.dbo.Customer, s511.dbo.[Transaction], s511.dbo.TransactionEntry, s511.dbo.Item, s511.dbo.Department
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Department.ID=Item.DepartmentID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use RetailDB
go
insert into TransactionEntries (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=566
go
delete from TempTxn
go

