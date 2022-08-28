select transactionentry.ID,CustomerID,Item.Description,TransactionTime,Quantity,Price,FullPrice,department.DepartmentName,
	discountcode.code as DiscountCode,returncode.code as ReturnCode, DiscountID, ReturnID
from transactionentry
left join item on item.ID = transactionentry.ItemID
left join department on department.ID = transactionentry.departmentid
left join reasoncode as discountcode on discountcode.ID = transactionentry.discountid
left join reasoncode as returncode on returncode.ID = transactionentry.returnid;