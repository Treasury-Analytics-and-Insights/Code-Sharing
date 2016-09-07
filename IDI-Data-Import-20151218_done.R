## ================================================ ##
##
## Author: Christopher Ball
## Date Created: 17/12/2015
##
## Purpose: To access IDI data sets through R studio web server.
## ================================================ ##

# Start by loading a "typical" IDI data set

library(RODBC)

if (require(RODBC)){
  connstr <- "DRIVER=ODBC Driver 11 for SQL Server; "
  connstr <- paste0(connstr, "Trusted_Connection=Yes; ")
  connstr <- paste0(connstr, "DATABASE=IDI_Clean_20150513 ; ")
  # Note that the port number is not obvious!
  connstr <- paste0(connstr, "SERVER=WPRDSQL36.stats.govt.nz, 49530")
  conn <- odbcDriverConnect(connection=connstr)
  # Check channel/connection information
  odbcGetInfo(conn)
  # Check available databases for the connection. Type Catalog into command window after running.
  Catalog <- sqlTables(conn, schema="", catalog="%", tableName="")
  # Find available tables within a catalog.  Type Tables into command window after running.
  Tables <- sqlTables(conn, catalog="IDI_Clean_20150513", tableName="%")
  # Finally extract information from a table.  Will default to the database selected in the connstr.
  dataframe <- sqlQuery(conn,"
                        SELECT TOP 1000 *
                        FROM
                        data.income_tax_yr_summary")
}

# On the off chance you are using R Studio without the web server use the code below...
if (require(RODBC) && F){
  Channel <- odbcDriverConnect("Driver=SQL Server; Server=WPRDSQL36\\iLEED; Database=IDI_Clean_20150513; trusted_connection=true;")
  odbcGetInfo(Channel)
  Catalog <- sqlTables(Channel, schema="", catalog="%", tableName="")
  Tables <- sqlTables(Channel, catalog="IDI_Clean_20150513", tableName="%")
  dataframe <- sqlQuery(Channel,"
                      SELECT TOP 1000 *
                        FROM
                        data.income_tax_yr_summary")
}
