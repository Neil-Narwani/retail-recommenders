use s310
go
Insert into RetailDB.dbo.TempTxn (CustomerID, ItemID, TransactionTime, Price, FullPrice, Quantity, DepartmentID, DiscountID, ReturnID)
select Customer.ID, Item.HQID as ItemIDX, [Transaction]."Time", TransactionEntry.Price, TransactionEntry.FullPrice, TransactionEntry.Quantity, Department.HQID, TransactionEntry.DiscountReasonCodeID, TransactionEntry.ReturnReasonCodeID
from s310.dbo.Customer, s310.dbo.[Transaction], s310.dbo.TransactionEntry, s310.dbo.Item, s310.dbo.Department
where s310.dbo.Customer.ID=s310.dbo.[Transaction].CustomerID and 
	s310.dbo.[Transaction].TransactionNumber = s310.dbo.TransactionEntry.TransactionNumber and
	s310.dbo.Item.ID=s310.dbo.TransactionEntry.ItemID and 
	s310.dbo.Department.ID=s310.dbo.Item.DepartmentID and
	s310.dbo.Customer.AccountNumber <> 'CASH' and s310.dbo.Customer.TotalVisits >= 3 and [Transaction]."Time" > '2008-01-01 00:00:00.000'
go 