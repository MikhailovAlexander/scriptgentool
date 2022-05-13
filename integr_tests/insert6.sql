SET IDENTITY_INSERT IntegrTestWork.dbo.Catalog ON;
INSERT INTO IntegrTestWork.dbo.Catalog(
    Catalog_id, Catalog_pid, Catalog_Name, Catalog_insDT, Catalog_updDT)
VALUES (1,NULL,'to insert 1',getdate(),getdate()),
       (2,NULL,'to insert 2',dateadd(day, -1, getdate()),dateadd(day, -1, getdate()));
SET IDENTITY_INSERT IntegrTestWork.dbo.Catalog OFF;