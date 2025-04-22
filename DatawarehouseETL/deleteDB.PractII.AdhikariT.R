# Header ------------------------------------------------------------------

# Program: deleteDB.PractII.AdhikariT.R 
# Name: Tejas Adhikari
# Semester: Spring 2025

# Header end --------------------------------------------------------------




# Install packages --------------------------------------------------------

# Install RMySQL package if not installed already
# This makes the code portable
if("RMySQL" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RMySQL")
}
# Load RMySQL Library
library(RMySQL)


# Install DBI package if not installed already
# This makes the code portable
if("DBI" %in% rownames(installed.packages()) == FALSE) {
  install.packages("DBI")
}
library(DBI)


# Connect to MySQL DB ----------------------------------------------------

#' This function helps connect to a dbfile in the
#' MySQL DB
#' 
#' @return returns a connection object.
connectDB <- function() {
  # Set up connection parameters.
  db_user <- ''
  db_password <- ''
  db_name <- ''
  db_host <- ''
  db_port <- 16950  
  
  # Connect to MySQL DB
  conn <- dbConnect(RMySQL::MySQL(), 
                    user = db_user, 
                    password = db_password, 
                    dbname = db_name, 
                    host = db_host, 
                    port = db_port)
  
  return(conn)
}


# Drop All Tables ---------------------------------------------------------

#' This Function drops all the tables from a given connection.
#' 
#' @param conn The connection object created on connection.
dropTables <- function(conn){
  # Get all table names
  tables <- dbListTables(conn)
  
  # Disable foreign key checks
  dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 0;")
  
  # Drop each table
  dbExecute(conn, sprintf("DROP TABLE IF EXISTS SalesFacts"))
  dbExecute(conn, sprintf("DROP TABLE IF EXISTS CustomerFacts"))
  dbExecute(conn, sprintf("DROP TABLE IF EXISTS CountryDimension"))
  dbExecute(conn, sprintf("DROP TABLE IF EXISTS PeriodDimension"))
  
  
  # Re-enable foreign key checks
  dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 1;")
}


# Delete from Tables ---------------------------------------------------------

#' This Function drops all the data from the tables from a given connection.
#' 
#' @param conn The connection object created on connection.
deleteFromTables <- function(conn){
  # Get all table names
  tables <- dbListTables(conn)
  
  # Disable foreign key checks
  dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 0;")
  
  # Drop each table
  dbExecute(conn, sprintf("DELETE FROM SalesFacts"))
  dbExecute(conn, sprintf("DELETE FROM CustomerFacts"))
  dbExecute(conn, sprintf("DELETE FROM CountryDimension"))
  dbExecute(conn, sprintf("DELETE FROM PeriodDimension"))
  
  
  # Re-enable foreign key checks
  dbExecute(conn, "SET FOREIGN_KEY_CHECKS = 1;")
}



main <- function() {
  # Get connection object
  conn = connectDB()
  
  # Delete All Tables in the DB
  dropTables(conn);
  
  # Print all the table that exist in the DB
  # Get all table names
  tables <- dbListTables(conn)
  cat("\nThe following tables are present in the DB: ")
  cat("\n", tables)
  
  # Disconnect from DB
  dbDisconnect(conn)
}


#####################################################################
main()


# Citations: --------------------------------------------------------------
## course materials: 
## https://northeastern.instructure.com/courses/206139/modules
## R documentation multiple pages: 
## https://www.rdocumentation.org/
## GeeksForGeeks gfg multiple pages: 
## https://www.geeksforgeeks.org/learn-r-programming/
## W3SChools multiple pages:
## https://www.w3schools.com/sql/