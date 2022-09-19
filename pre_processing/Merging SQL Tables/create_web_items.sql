drop temporary table if exists foo;
drop table if exists items_web;
create temporary table foo select distinct itemid,departmentid from transactionentry where TransactionTime > '2017-01-01 00:00:00' limit 29000;
create table items_web select item.id as id, Description as description, departmentid as department_id, fullprice as price from item,foo where item.id = foo.itemid;
ALTER TABLE `retaildb`.`items_web` 
ADD INDEX `fc_itemweb_deptid_idx` (`department_id` ASC) VISIBLE;
;
ALTER TABLE `retaildb`.`items_web` 
ADD CONSTRAINT `fc_itemweb_deptid`
  FOREIGN KEY (`department_id`)
  REFERENCES `retaildb`.`department` (`ID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;