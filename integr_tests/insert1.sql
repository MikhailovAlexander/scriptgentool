SET IDENTITY_INSERT IntegrTestClear.dbo.Catalog ON;
INSERT INTO IntegrTestClear.dbo.Catalog(
    Catalog_id, Catalog_pid, Catalog_Name, Catalog_insDT, Catalog_updDT)
VALUES (2,NULL,'to delete',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.Catalog OFF;

SET IDENTITY_INSERT IntegrTestWork.dbo.Catalog ON;
INSERT INTO IntegrTestWork.dbo.Catalog(
    Catalog_id, Catalog_pid, Catalog_Name, Catalog_insDT, Catalog_updDT)
VALUES (1,NULL,'to insert',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestWork.dbo.Catalog OFF;