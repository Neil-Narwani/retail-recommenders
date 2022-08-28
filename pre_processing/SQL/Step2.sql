use RetailDB
go
insert into RetailDB.dbo.Customer (StoreID,OldCustomerID, Company, FirstName, LastName, "Address", Address2, City, "State", Zip, PhoneNumber, EmailAddress, BirthMonth,TotalSales,TotalSavings,TotalVisits,LastVisit, ReferralCode)
select '310' as StoreID, Customer.ID as OldCustomerID, Company, FirstName, LastName, "Address", Address2, City, "State", Zip, PhoneNumber, EmailAddress, CustomNumber1 as BirthMonth, TotalSales, TotalSavings, TotalVisits, LastVisit, CustomText1 as ReferralCode
from s310.dbo.Customer
where s310.dbo.Customer.ID in (select distinct CustomerID from TempTxn)
go
