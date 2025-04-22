
# Header ------------------------------------------------------------------

# Program: createDB.PractI.AdhikariT.R 
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


# Create new Tables in the DB ---------------------------------------------
#'This finction creates all the required tables
#'for the relation.
#'
#'@param conn takes in the connection Object.
createTables <- function(conn){
  # # Enable foreign key constraints
  # dbExecute(conn, "PRAGMA foreign_keys = ON")
  
  # Create Restaurants table
  sql <- "CREATE TABLE IF NOT EXISTS Restaurants (
            RestaurantID INTEGER PRIMARY KEY,
            RestaurantName VARCHAR(100) NOT NULL
          );"
  dbExecute(conn, sql)
  
  
  # Create Servers table
  sql <- "CREATE TABLE IF NOT EXISTS Servers (
            ServerEmpID INTEGER PRIMARY KEY,
            ServerName VARCHAR(100) DEFAULT 'N/A',
            HourlyRate FLOAT DEFAULT 0.0,
            StartDateHired DATE,
            EndDateHired DATE DEFAULT NULL,
            ServerBirthDate DATE DEFAULT NULL,
            ServerTIN VARCHAR(12) DEFAULT NULL,
            CONSTRAINT check_hourly_rate CHECK (HourlyRate >= 0)
          );"
  dbExecute(conn, sql)
  
  
  # Create Customers Table
  sql <- "CREATE TABLE IF NOT EXISTS Customers (
            CustomerID INTEGER PRIMARY KEY,
            CustomerName VARCHAR(100) DEFAULT NULL,
            CustomerPhone VARCHAR(15) DEFAULT NULL,
            CustomerEmail VARCHAR(100) DEFAULT NULL,
            LoyaltyMember BOOLEAN DEFAULT FALSE,
            CONSTRAINT valid_email CHECK (
              CustomerEmail IS NULL OR CustomerEmail LIKE '%@%.%'
            )
          );"
  dbExecute(conn, sql)
  
  
  # Create Payments Table
  sql <-"CREATE TABLE IF NOT EXISTS Payments (
          PaymentID INTEGER PRIMARY KEY,
          PaymentMethod VARCHAR(50) NOT NULL UNIQUE
        );"
  dbExecute(conn, sql)
  
  
  # Create MealTypes table
  sql <- "CREATE TABLE IF NOT EXISTS MealTypes (
            MealTypeID INTEGER PRIMARY KEY,
            MealType VARCHAR(50) NOT NULL UNIQUE
          );"
  dbExecute(conn, sql)
  
  
  # Create AlcoholOrders table
  sql <- "CREATE TABLE IF NOT EXISTS AlcoholOrders (
            AlcoholOrderID INTEGER PRIMARY KEY,
            OrderedAlcohol BOOLEAN NOT NULL DEFAULT FALSE,
            AlcoholBill FLOAT NOT NULL DEFAULT 0.0,
            CONSTRAINT check_alcoholbill CHECK (AlcoholBill >= 0)
          );"
  dbExecute(conn, sql)
  
  
  # Create FoodOrders table
  sql <- "
          CREATE TABLE IF NOT EXISTS FoodOrders (
              FoodOrderID INTEGER PRIMARY KEY,
              MealTypeID INTEGER NOT NULL,
              FoodBill FLOAT NOT NULL DEFAULT 0.0,
              CONSTRAINT fk_mealtype FOREIGN KEY (MealTypeID) REFERENCES MealTypes(MealTypeID),
              CONSTRAINT check_foodbill CHECK (FoodBill >= 0)
          );"
  dbExecute(conn, sql)
  
  
  # Create Orders table
  sql <- "CREATE TABLE IF NOT EXISTS Orders (
              OrderID INTEGER PRIMARY KEY,
              FoodOrderID INTEGER,
              AlcoholOrderID INTEGER,
              TotalBill FLOAT NOT NULL DEFAULT 0.0,
              CONSTRAINT fk_foodorder FOREIGN KEY (FoodOrderID) REFERENCES FoodOrders(FoodOrderID),
              CONSTRAINT fk_alcoholorder FOREIGN KEY (AlcoholOrderID) REFERENCES AlcoholOrders(AlcoholOrderID),
              CONSTRAINT check_totalbill CHECK (TotalBill >= 0)
          );"
  dbExecute(conn, sql)
  
  
  # Create Visits table
  sql <- "CREATE TABLE IF NOT EXISTS Visits (
            VisitID INTEGER PRIMARY KEY,
            RestaurantID INTEGER NOT NULL,
            ServerEmpID INTEGER,
            CustomerID INTEGER,
            OrderID INTEGER,
            PaymentID INTEGER DEFAULT 3,
            VisitDate DATE NOT NULL,
            VisitTime TIME NOT NULL,
            PartySize INTEGER DEFAULT 1,
            Genders VARCHAR(1000) DEFAULT 'u',
            WaitTime INTEGER DEFAULT 0,
            TipAmount FLOAT DEFAULT 0.0,
            DiscountApplied FLOAT DEFAULT 0.0,
            CONSTRAINT fk_restaurant FOREIGN KEY (RestaurantID) REFERENCES Restaurants(RestaurantID),
            CONSTRAINT fk_server FOREIGN KEY (ServerEmpID) REFERENCES Servers(ServerEmpID),
            CONSTRAINT fk_customer FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
            CONSTRAINT fk_order FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
            CONSTRAINT fk_payment FOREIGN KEY (PaymentID) REFERENCES Payments(PaymentID),
            CONSTRAINT check_partysize CHECK (PartySize > 0 AND PartySize <= 1000),
            CONSTRAINT check_waittime CHECK (WaitTime >= 0),
            CONSTRAINT check_tipamount CHECK (TipAmount >= 0)
          );"
  dbExecute(conn, sql)
}


main <- function() {
  # Get connection object
  conn = connectDB()
  print(conn)
  
  # Add tables to the DB
  createTables(conn);
  
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

