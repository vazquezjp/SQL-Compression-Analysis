USE [DBA]
GO

/****** Object:  Table [dbo].[tblCompressAnalysis]    Script Date: 10/27/2014 15:18:24 ******/
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;

CREATE TABLE [dbo].[tblCompressAnalysis](
	[PK] [int] IDENTITY(1,1) NOT NULL,
	[database_name] [varchar](250) NULL,
	[schema_name] [varchar](250) NULL,
	[object_name] [nvarchar](150) NULL,
	[savings_row_percent] [smallint] NULL,
	[savings_page_percent] [smallint] NULL,
	[s_val] [decimal](5, 2) NULL,
	[u_val] [decimal](5, 2) NULL,
	[decision] [varchar](10) NULL,
	[notes] [varchar](255) NULL,
	[index_id] [int] NULL,
	[ixName] [varchar](255) NULL,
	[data_compression_desc] [varchar](50) NULL,
	[None_Size] [int] NULL,
	[Row_Size] [int] NULL,
	[Page_Size] [int] NULL,
	[partition_number] [int] NULL,
	[ixType] [varchar](50) NULL,
	[dtCreated] [smalldatetime] NULL,
 CONSTRAINT [PK_tblCompressAnalysis] PRIMARY KEY CLUSTERED 
(
	[PK] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY];

SET ANSI_PADDING OFF;

ALTER TABLE [dbo].[tblCompressAnalysis] ADD  CONSTRAINT [DF_tblCompressAnalysis_dtCreated]  DEFAULT (getdate()) FOR [dtCreated];

