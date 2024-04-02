USE WatchStore
ALTER TABLE [Review] ADD CreatedBy varchar(32) NOT NULL DEFAULT ''; 
CREATE TABLE [test] ( tmp int);
ALTER TABLE [test] ADD dsfsd smalldatetime;
ALTER TABLE [test] ADD haha nchar(10) NOT NULL DEFAULT ''; 
ALTER TABLE [test] ADD CONSTRAINT PK_3a85997cce34440992b88ed8f1d18e93 PRIMARY KEY (haha);
ALTER TABLE [test] ADD dsfsdfsdvxc bit;
ALTER TABLE [test] ADD sdfsd real;
ALTER TABLE [test] ADD sothuc decimal(18, 0);
ALTER TABLE [test] ADD hihi nvarchar(MAX);
ALTER TABLE [test] DROP COLUMN tmp;
