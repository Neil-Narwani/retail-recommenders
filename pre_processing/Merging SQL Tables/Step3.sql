use RetailDB
go
insert into TransactionEntry (CustomerID, ItemID, TransactionTime, Price, FullPrice, Quantity, DepartmentID, DiscountID, ReturnID)
select Customer.ID, ItemID, TransactionTime, Price, FullPrice, Quantity, DepartmentID, DiscountID, ReturnID
from TempTxn
inner join Customer on Customer.OldCustomerID=TempTxn.CustomerID
where Customer.StoreID=310
go