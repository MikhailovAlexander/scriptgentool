CREATE LOGIN IntegrTest
    WITH PASSWORD = '#IntegrTest2545';
GO

CREATE DATABASE [IntegrTestClear]
GO

USE [IntegrTestClear]
GO

CREATE USER IntegrTest FOR LOGIN IntegrTest;
GO

GRANT ALL TO IntegrTest;
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Catalog](
	[Catalog_id] [bigint] IDENTITY(1,1) NOT NULL,
	[Catalog_pid] [bigint] NULL,
	[Catalog_Name] [varchar](200) NOT NULL,
	[Catalog_insDT] [datetime] NULL,
	[Catalog_updDT] [datetime] NULL,
 CONSTRAINT [PK_Catalog] PRIMARY KEY CLUSTERED
(
	[Catalog_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Parameter](
  [Parameter_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Parameter_Name] [varchar](50) NOT NULL,
  [Parameter_Label] [varchar](100) NOT NULL,
  [Parameter_Default] [varchar](100) NULL,
  [Parameter_insDT] [datetime] NULL,
  [Parameter_updDT] [datetime] NULL,
 CONSTRAINT [PK_Parameter] PRIMARY KEY CLUSTERED
(
  [Parameter_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Report](
  [Report_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Catalog_id] [bigint] NOT NULL,
  [Report_Title] [varchar](300) NULL,
  [Report_FileName] [varchar](80) NOT NULL,
  [Report_insDT] [datetime] NULL,
  [Report_updDT] [datetime] NULL,
 CONSTRAINT [PK_Report] PRIMARY KEY CLUSTERED
(
  [Report_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Report]  WITH NOCHECK ADD  CONSTRAINT [fk_Report_Catalog_id] FOREIGN KEY([Catalog_id])
REFERENCES [dbo].[Catalog] ([Catalog_id])
GO

ALTER TABLE [dbo].[Report] CHECK CONSTRAINT [fk_Report_Catalog_id]
GO

CREATE TABLE [dbo].[ReportParameterLink](
  [ReportParameterLink_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Report_id] [bigint] NOT NULL,
  [Parameter_id] [bigint] NULL,
  [ReportParameterLink_insDT] [datetime] NULL,
  [ReportParameterLink_updDT] [datetime] NULL,
 CONSTRAINT [PK_ReportParameterLink] PRIMARY KEY CLUSTERED
(
  [ReportParameterLink_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ReportParameterLink]  WITH NOCHECK ADD  CONSTRAINT [fk_ReportParameterLink_Report_id] FOREIGN KEY([Report_id])
REFERENCES [dbo].[Report] ([Report_id])
GO

ALTER TABLE [dbo].[ReportParameterLink] CHECK CONSTRAINT [fk_ReportParameterLink_Report_id]
GO

ALTER TABLE [dbo].[ReportParameterLink]  WITH NOCHECK ADD  CONSTRAINT [fk_ReportParameterLink_Parameter_id] FOREIGN KEY([Parameter_id])
REFERENCES [dbo].[Parameter] ([Parameter_id])
GO

ALTER TABLE [dbo].[ReportParameterLink] CHECK CONSTRAINT [fk_ReportParameterLink_Parameter_id]
GO

CREATE TABLE [dbo].[RunHistory](
  [RunHistory_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Report_id] [bigint] NOT NULL,
  [RunHistory_begDT] [datetime] NULL,
  [RunHistory_endDT] [datetime] NULL,
  [RunHistory_insDT] [datetime] NULL,
  [RunHistory_updDT] [datetime] NULL,
 CONSTRAINT [PK_RunHistory] PRIMARY KEY CLUSTERED
(
  [RunHistory_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[RunHistory]  WITH NOCHECK ADD  CONSTRAINT [fk_RunHistory_Report_id] FOREIGN KEY([Report_id])
REFERENCES [dbo].[Report] ([Report_id])
GO

ALTER TABLE [dbo].[RunHistory] CHECK CONSTRAINT [fk_RunHistory_Report_id]
GO

CREATE DATABASE [IntegrTestWork]
GO

USE [IntegrTestWork]
GO

CREATE USER IntegrTest FOR LOGIN IntegrTest;
GO

GRANT ALL TO IntegrTest;
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Catalog](
	[Catalog_id] [bigint] IDENTITY(1,1) NOT NULL,
	[Catalog_pid] [bigint] NULL,
	[Catalog_Name] [varchar](200) NOT NULL,
	[Catalog_insDT] [datetime] NULL,
	[Catalog_updDT] [datetime] NULL,
 CONSTRAINT [PK_Catalog] PRIMARY KEY CLUSTERED
(
	[Catalog_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Parameter](
  [Parameter_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Parameter_Name] [varchar](50) NOT NULL,
  [Parameter_Label] [varchar](100) NOT NULL,
  [Parameter_Default] [varchar](100) NULL,
  [Parameter_insDT] [datetime] NULL,
  [Parameter_updDT] [datetime] NULL,
 CONSTRAINT [PK_Parameter] PRIMARY KEY CLUSTERED
(
  [Parameter_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Report](
  [Report_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Catalog_id] [bigint] NOT NULL,
  [Report_Title] [varchar](300) NULL,
  [Report_FileName] [varchar](80) NOT NULL,
  [Report_insDT] [datetime] NULL,
  [Report_updDT] [datetime] NULL,
 CONSTRAINT [PK_Report] PRIMARY KEY CLUSTERED
(
  [Report_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Report]  WITH NOCHECK ADD  CONSTRAINT [fk_Report_Catalog_id] FOREIGN KEY([Catalog_id])
REFERENCES [dbo].[Catalog] ([Catalog_id])
GO

ALTER TABLE [dbo].[Report] CHECK CONSTRAINT [fk_Report_Catalog_id]
GO

CREATE TABLE [dbo].[ReportParameterLink](
  [ReportParameterLink_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Report_id] [bigint] NOT NULL,
  [Parameter_id] [bigint] NULL,
  [ReportParameterLink_insDT] [datetime] NULL,
  [ReportParameterLink_updDT] [datetime] NULL,
 CONSTRAINT [PK_ReportParameterLink] PRIMARY KEY CLUSTERED
(
  [ReportParameterLink_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ReportParameterLink]  WITH NOCHECK ADD  CONSTRAINT [fk_ReportParameterLink_Report_id] FOREIGN KEY([Report_id])
REFERENCES [dbo].[Report] ([Report_id])
GO

ALTER TABLE [dbo].[ReportParameterLink] CHECK CONSTRAINT [fk_ReportParameterLink_Report_id]
GO

ALTER TABLE [dbo].[ReportParameterLink]  WITH NOCHECK ADD  CONSTRAINT [fk_ReportParameterLink_Parameter_id] FOREIGN KEY([Parameter_id])
REFERENCES [dbo].[Parameter] ([Parameter_id])
GO

ALTER TABLE [dbo].[ReportParameterLink] CHECK CONSTRAINT [fk_ReportParameterLink_Parameter_id]
GO

CREATE TABLE [dbo].[RunHistory](
  [RunHistory_id] [bigint] IDENTITY(1,1) NOT NULL,
  [Report_id] [bigint] NOT NULL,
  [RunHistory_begDT] [datetime] NULL,
  [RunHistory_endDT] [datetime] NULL,
  [RunHistory_insDT] [datetime] NULL,
  [RunHistory_updDT] [datetime] NULL,
 CONSTRAINT [PK_RunHistory] PRIMARY KEY CLUSTERED
(
  [RunHistory_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[RunHistory]  WITH NOCHECK ADD  CONSTRAINT [fk_RunHistory_Report_id] FOREIGN KEY([Report_id])
REFERENCES [dbo].[Report] ([Report_id])
GO

ALTER TABLE [dbo].[RunHistory] CHECK CONSTRAINT [fk_RunHistory_Report_id]
GO
