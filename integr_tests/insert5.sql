SET IDENTITY_INSERT IntegrTestClear.dbo.Catalog ON;
INSERT INTO IntegrTestClear.dbo.Catalog(
    Catalog_id, Catalog_pid, Catalog_Name, Catalog_insDT, Catalog_updDT)
VALUES (2,NULL,'to delete 2',getdate(),getdate()),
       (3,NULL,'to delete 3',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.Catalog OFF;