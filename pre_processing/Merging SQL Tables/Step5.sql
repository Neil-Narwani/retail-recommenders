use RetailDB
go

set identity_insert Department ON
go
Insert Into Department (ID, DepartmentName)
	Select HQID,Name from s501.dbo.Department
go
set identity_insert Department OFF
go
set identity_insert ReasonCode ON
go
insert into ReasonCode (ID, Code, "Description")
	Select HQID,Code,"Description" from s501.dbo.ReasonCode
go
set identity_insert ReasonCode OFF
go
set identity_insert Item ON
go
insert into Item (ID, "Description", FullPrice, BrandCode)
	Select HQID, "Description", Price,BinLocation from s501.dbo.Item
go
set identity_insert Item OFF
go

