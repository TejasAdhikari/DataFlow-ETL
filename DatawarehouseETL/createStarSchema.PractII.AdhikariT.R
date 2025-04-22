# Header ------------------------------------------------------------------

# Program: createStarSchema.PractII.AdhikariT.R 
# Name: Tejas Adhikari
# Semester: Spring 2025

# Header end --------------------------------------------------------------



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


# Create new Fact Tables in the DB ---------------------------------------------
#'This finction creates all the required fact tables
#'for the OLAP processes.
#'
#'@param conn takes in the connection Object.
createFactTables <- function(conn){
  # # Enable foreign key constraints
  # dbExecute(conn, "PRAGMA foreign_keys = ON")
  
  
  # Create CountryDimension table
  sql <- "CREATE TABLE IF NOT EXISTS CountryDimension (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            Country VARCHAR(200) UNIQUE NOT NULL
          );"
  dbExecute(conn, sql)
  
  
  # Create PeriodDimension table
  sql <- "CREATE TABLE IF NOT EXISTS PeriodDimension (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            Month INTEGER NOT NULL,
            YEAR INTEGER NOT NULL,
            QUARTER INTEGER NOT NULL,
            UNIQUE KEY period_unique (Month, YEAR, QUARTER)
          );"
  dbExecute(conn, sql)
  
  
  # Create SalesFacts table
  sql <- "CREATE TABLE IF NOT EXISTS SalesFacts (
            factId INTEGER AUTO_INCREMENT PRIMARY KEY,
            MonthlyRevenue DECIMAL(65, 2) NOT NULL,
            MonthlyUnitsSold INTEGER NOT NULL,
            PeriodId INTEGER NOT NULL,
            CountryId INTEGER NOT NULL,
            Type VARCHAR(250) NOT NULL,
            CONSTRAINT fk_country FOREIGN KEY (CountryId) REFERENCES CountryDimension(id),
            CONSTRAINT fk_period FOREIGN KEY (PeriodId) REFERENCES PeriodDimension(id)
          );"
  dbExecute(conn, sql)
  
  
  # Create CustomerFacts table
  sql <- "CREATE TABLE IF NOT EXISTS CustomerFacts (
            factId INTEGER AUTO_INCREMENT PRIMARY KEY,
            CustomerCount INTEGER NOT NULL,
            CountryId INTEGER NOT NULL,
            Type VARCHAR(250) NOT NULL,
            CONSTRAINT fk_country_customer FOREIGN KEY (CountryId) REFERENCES CountryDimension(id)
          );"
  dbExecute(conn, sql)
}



main <- function() {
  # Connect to MySQL DB
  connMySQL = connectDB()
  
  # Create tables in the DB
  createFactTables(connMySQL)
  
  # Display the tables
  MySQLtables <- dbListTables(connMySQL)
  print(MySQLtables)
  
  # Disconnect from MySQL DB
  dbDisconnect(connMySQL)
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