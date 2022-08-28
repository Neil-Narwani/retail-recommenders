USE [RetailDB]
GO

/****** Object:  Table [dbo].[Customer]    Script Date: 7/31/2022 9:57:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Customer](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Zip] [nvarchar](15) NULL,
	[StoreID] [int] NULL,
	[OldCustomerID] [int] NULL,
	[BirthMonth] [int] NULL,
	[TotalSales] [money] NULL,
	[TotalSavings] [money] NULL,
	[TotalVisits] [int] NULL,
	[LastVisit] [datetime] NULL,
	[ReferralCode] [nvarchar](50) NULL,
	[Company] [nvarchar](50) NULL,
	[FirstName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NULL,
	[Address] [nvarchar](50) NULL,
	[Address2] [nvarchar](50) NULL,
	[City] [nvarchar](50) NULL,
	[State] [nvarchar](50) NULL,
	[PhoneNumber] [nvarchar](50) NULL,
	[EmailAddress] [nvarchar](255) NULL,
 CONSTRAINT [PK_Customer] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[Department](
	[ID] [int] NOT NULL,
	[DepartmentName] [nvarchar](50) NULL,
 CONSTRAINT [PK_Departments] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[Item](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Description] [nvarchar](30) NULL,
	[FullPrice] [money] NULL,
 CONSTRAINT [PK_Items] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[ReasonCode](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Code] [nvarchar](25) NULL,
	[Description] [nvarchar](50) NULL,
 CONSTRAINT [PK_ReasonCodes] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[TransactionEntry](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NOT NULL,
	[ItemID] [int] NOT NULL,
	[TransactionTime] [datetime] NULL,
	[Price] [money] NULL,
	[FullPrice] [money] NULL,
	[Quantity] [float] NULL,
	[DepartmentID] [int] NOT NULL,
	[DiscountID] [int] NULL,
	[ReturnID] [int] NULL,
 CONSTRAINT [PK_TransactionEntry] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [dbo].[TransactionEntry]  WITH CHECK ADD  CONSTRAINT [FK_Trans_Customer] FOREIGN KEY([CustomerID])
REFERENCES [dbo].[Customer] ([ID])
GO

ALTER TABLE [dbo].[TransactionEntry] CHECK CONSTRAINT [FK_Trans_Customer]
GO

ALTER TABLE [dbo].[TransactionEntry]  WITH CHECK ADD  CONSTRAINT [FK_Trans_Department] FOREIGN KEY([DepartmentID])
REFERENCES [dbo].[Department] ([ID])
GO

ALTER TABLE [dbo].[TransactionEntry] CHECK CONSTRAINT [FK_Trans_Department]
GO

ALTER TABLE [dbo].[TransactionEntry]  WITH CHECK ADD  CONSTRAINT [FK_Trans_Item] FOREIGN KEY([ItemID])
REFERENCES [dbo].[Item] ([ID])
GO

ALTER TABLE [dbo].[TransactionEntry] CHECK CONSTRAINT [FK_Trans_Item]
GO



