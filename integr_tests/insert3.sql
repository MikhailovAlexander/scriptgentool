SET IDENTITY_INSERT IntegrTestClear.dbo.Catalog ON;
INSERT INTO IntegrTestClear.dbo.Catalog(
    Catalog_id, Catalog_pid, Catalog_Name, Catalog_insDT, Catalog_updDT)
VALUES (3,NULL,'to delete 3',getdate(),getdate()),
       (4,NULL,'to delete 4',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.Catalog OFF;

SET IDENTITY_INSERT IntegrTestWork.dbo.Catalog ON;
INSERT INTO IntegrTestWork.dbo.Catalog(
    Catalog_id, Catalog_pid, Catalog_Name, Catalog_insDT, Catalog_updDT)
VALUES (3,NULL,'to delete 3',getdate(),getdate()),
       (4,NULL,'to delete 4',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestWork.dbo.Catalog OFF;

SET IDENTITY_INSERT IntegrTestClear.dbo.Parameter ON;
INSERT INTO IntegrTestClear.dbo.Parameter(
    Parameter_id, Parameter_Name, Parameter_Label, Parameter_Default,
    Parameter_insDT, Parameter_updDT)
VALUES (3,'to delete 3','3',NULL,getdate(),getdate()),
       (4,'to delete 3','4',NULL,getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.Parameter OFF;

SET IDENTITY_INSERT IntegrTestWork.dbo.Parameter ON;
INSERT INTO IntegrTestWork.dbo.Parameter(
    Parameter_id, Parameter_Name, Parameter_Label, Parameter_Default,
    Parameter_insDT, Parameter_updDT)
VALUES (3,'to delete 3','3',NULL,getdate(),getdate()),
       (4,'to delete 3','4',NULL,getdate(),getdate());
SET IDENTITY_INSERT IntegrTestWork.dbo.Parameter OFF;

SET IDENTITY_INSERT IntegrTestClear.dbo.Report ON;
INSERT INTO IntegrTestClear.dbo.Report(
    Report_id, Catalog_id, Report_Title, Report_FileName,
    Report_insDT, Report_updDT)
VALUES (3,3,'to delete 3','test',getdate(),getdate()),
       (4,4,'to delete 3','test',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.Report OFF;

SET IDENTITY_INSERT IntegrTestWork.dbo.Report ON;
INSERT INTO IntegrTestWork.dbo.Report(
    Report_id, Catalog_id, Report_Title, Report_FileName,
    Report_insDT, Report_updDT)
VALUES (3,3,'to delete 3','test',getdate(),getdate()),
       (4,4,'to delete 3','test',getdate(),getdate());
SET IDENTITY_INSERT IntegrTestWork.dbo.Report OFF;

SET IDENTITY_INSERT IntegrTestClear.dbo.ReportParameterLink ON;
INSERT INTO IntegrTestClear.dbo.ReportParameterLink(
    ReportParameterLink_id, Report_id, Parameter_id, ReportParameterLink_insDT,
    ReportParameterLink_updDT)
VALUES (3,3,3,getdate(),getdate()),
       (4,4,4,getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.ReportParameterLink OFF;

SET IDENTITY_INSERT IntegrTestWork.dbo.ReportParameterLink ON;
INSERT INTO IntegrTestWork.dbo.ReportParameterLink(
    ReportParameterLink_id, Report_id, Parameter_id, ReportParameterLink_insDT,
    ReportParameterLink_updDT)
VALUES (3,3,3,getdate(),getdate()),
       (4,4,4,getdate(),getdate());
SET IDENTITY_INSERT IntegrTestWork.dbo.ReportParameterLink OFF;

SET IDENTITY_INSERT IntegrTestClear.dbo.RunHistory ON;
INSERT INTO IntegrTestClear.dbo.RunHistory(
    RunHistory_id, Report_id, RunHistory_begDT, RunHistory_endDT,
    RunHistory_insDT, RunHistory_updDT)
VALUES (3,3,getdate(),getdate(),getdate(),getdate()),
       (4,4,getdate(),getdate(),getdate(),getdate());
SET IDENTITY_INSERT IntegrTestClear.dbo.RunHistory OFF;

SET IDENTITY_INSERT IntegrTestWork.dbo.RunHistory ON;
INSERT INTO IntegrTestWork.dbo.RunHistory(
    RunHistory_id, Report_id, RunHistory_begDT, RunHistory_endDT,
    RunHistory_insDT, RunHistory_updDT)
VALUES (3,3,getdate(),getdate(),getdate(),getdate()),
       (4,4,getdate(),getdate(),getdate(),getdate());
SET IDENTITY_INSERT IntegrTestWork.dbo.RunHistory OFF;