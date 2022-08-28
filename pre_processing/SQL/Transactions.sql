use s511
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemLookupCode, "Description", TransactionTime, Price, FullPrice, Quantity, Department.Name, DiscountID, ReturnID)
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.Quantity, Category.Name
from Customer, [Transaction], TransactionEntry, Item, Category
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Category.ID=Item.CategoryID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use s512
go
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.Quantity, Category.Name
from Customer, [Transaction], TransactionEntry, Item, Category
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Category.ID=Item.CategoryID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go
use s582
go
select Customer.ID, ItemLookupCode, Item."Description", [Transaction]."Time", TransactionEntry.Price, TransactionEntry.Quantity, Category.Name
from Customer, [Transaction], TransactionEntry, Item, Category
where Customer.ID=[Transaction].CustomerID and 
	[Transaction].TransactionNumber=TransactionEntry.TransactionNumber and
	Item.ID=TransactionEntry.ItemID and 
	Category.ID=Item.CategoryID and
	Customer.AccountNumber <> 'CASH' and TotalVisits >= 3
go