# Header ------------------------------------------------------------------

# Program: testDBLoading.PractI.AdhikariT.R 
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



# Test Restaurants --------------------------------------------------------

#' This function test the number of unique restaurants in the 
#' csv and the DB.
#' 
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testRestaurants <- function(conn, df.orig) {
  print("Testing Unique Restaurants:")
  # Count unique restaurants in the CSV
  num_unique_restaurants_csv <- length(unique(df.orig$Restaurant))
  cat("Number of unique restaurants in CSV:", num_unique_restaurants_csv, "\n")
  
  # Query the database for count of unique entries in RestaurantName column
  sql <- "SELECT COUNT(DISTINCT RestaurantName) AS uniqueRestaurants 
          FROM Restaurants;"
  result <- dbGetQuery(conn, sql)
  
  # Extract the number of unique restaurants from the query result
  num_unique_restaurants_db <- result$uniqueRestaurants[1]
  cat("Number of unique restaurants in Database:", num_unique_restaurants_db, "\n")
  
  # Compare the results from csv and the DB
  if (num_unique_restaurants_csv == num_unique_restaurants_db) {
    cat("Test PASSED: The number of unique restaurants match in csv and DB.\n\n")
  } 
  else {
    cat("Test FAILED: CSV has", num_unique_restaurants_csv, 
        "unique restaurants but the database has", num_unique_restaurants_db, ".\n\n")
  }
  
  
}


# Test customers --------------------------------------------------------

#' This function test the number of unique customers in the 
#' csv and the DB.
#' 
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testCustomers <- function(conn, df.orig) {
  print("Testing Unique Customers:")
  # Extract and count unique customers by email or phone
  ## we are filtering out the "" string.
  filtered_customers_csv <- df.orig$CustomerName[df.orig$CustomerName != ""]
  num_unique_customers_csv <- length(unique(filtered_customers_csv))
  
  # Print the result
  cat("Number of unique customers in CSV:", num_unique_customers_csv, "\n")
  
  
  # Query the database for count of unique entries in Customers Table.
  sql <- "SELECT COUNT(*) AS uniqueCustomers 
          FROM Customers;"
  result <- dbGetQuery(conn, sql)
  
  # Extract the number of unique restaurants from the query result
  num_unique_customers_db <- result$uniqueCustomers[1]
  cat("Number of unique restaurants in Database:", num_unique_customers_db, "\n")
  
  # Compare the results from csv and the DB
  if (num_unique_customers_csv == num_unique_customers_db) {
    cat("Test PASSED: The number of unique customers match in csv and DB.\n\n")
  } 
  else {
    cat("Test FAILED: CSV has", num_unique_customers_csv, 
        "unique customers but the database has", num_unique_customers_db, ".\n\n")
  }
  
}


# Test Servers --------------------------------------------------------

#' This function test the number of unique restaurants in the
#' csv and the DB.
#'
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testServers <- function(conn, df.orig) {
  print("Testing Unique Servers:")
  # Extract and count unique servers
  ## we are filtering out the NA and NULL calues.
  filtered_servers <- df.orig$ServerEmpID[!is.na(df.orig$ServerEmpID) & df.orig$ServerEmpID != "NULL"]
  unique_servers_csv <- unique(filtered_servers)
  num_unique_servers_csv <- length(unique_servers_csv)
  
  cat("Number of unique servers in CSV:", num_unique_servers_csv, "\n")

  # Query the database for count of unique entries in RestaurantName column
  sql <- "SELECT COUNT(*) AS uniqueServers
          FROM Servers;"
  result <- dbGetQuery(conn, sql)

  # Extract the number of unique restaurants from the query result
  num_unique_servers_db <- result$uniqueServers[1]
  cat("Number of unique restaurants in Database:", num_unique_servers_db, "\n")

  # Compare the results from csv and the DB
  if (num_unique_servers_csv == num_unique_servers_db) {
    cat("Test PASSED: The number of unique restaurants match in csv and DB.\n\n")
  }
  else {
    cat("Test FAILED: CSV has", num_unique_servers_csv,
        "unique restaurants but the database has", num_unique_servers_db, ".\n\n")
  }

}


# Test Visits --------------------------------------------------------

#' This function test the number of unique Visits in the
#' csv and the DB.
#'
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testVisits <- function(conn, df.orig) {
  print("Testing Number of unique Visits:")
  
  # Count unique Visits in the CSV
  num_unique_visits_csv <- length(df.orig$VisitID)
  cat("Number of unique Visits in CSV:", num_unique_visits_csv, "\n")

  # Query the database for count of unique entries in Visits
  sql <- "SELECT COUNT(*) AS uniqueVisits
          FROM Visits;"
  result <- dbGetQuery(conn, sql)

  # Extract the number of unique Visits from the query result
  num_unique_visits_db <- result$uniqueVisits[1]
  cat("Number of unique Visits in Database:", num_unique_visits_db, "\n")

  # Compare the results from csv and the DB
  if (num_unique_visits_csv == num_unique_visits_db) {
    cat("Test PASSED: The number of unique Visits match in csv and DB.\n\n")
  }
  else {
    cat("Test FAILED: CSV has", num_unique_visits_csv,
        "unique Visits but the database has", num_unique_visits_db, ".\n\n")
  }

}


# Test Sum up the total amount spent on food --------------------------

#' This function tests the Sum up the total amount spent on food in the
#' csv and the DB.
#'
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testSumFoodAmount <- function(conn, df.orig) {
  print("Testing sum of food bill:")
  # Calculate the total amount spent on food in the CSV
  total_food_amount_csv <- sum(df.orig$FoodBill, na.rm = TRUE)
  
  # Print the result
  cat("Total amount spent on food in CSV:", total_food_amount_csv, "\n")
  
  
  # Query the database for sum of food bill
  sql <- "SELECT SUM(FoodBill) AS TotalFoodBill
          FROM FoodOrders;"
  result <- dbGetQuery(conn, sql)
  
  # Extract the sum of food bill from the query result
  total_food_amount_db <- result$TotalFoodBill[1]
  cat("Total amount spent on food in Database:", total_food_amount_db, "\n")
  
  # Compare the results from csv and the DB
  ## all.equal is used to compare floating points with small differences in the decimal points,
  if (isTRUE(all.equal(total_food_amount_csv, total_food_amount_db))) {
    cat("Test PASSED: The total amount spent on food matches in the csv and the Database.\n\n")
  }
  else {
    cat("Test FAILED: CSV food total is", total_food_amount_csv,
        "but the food total is", total_food_amount_db, ".\n\n")
  }
  
}


# Test Sum up the total amount spent on alcohol --------------------------

#' This function tests the Sum up the total amount spent on alcohol in the
#' csv and the DB.
#'
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testSumAlcoholAmount <- function(conn, df.orig) {
  print("Testing sum of alcohol bill:")
  # Calculate the total amount spent on alcohol in the CSV
  total_alcohol_amount_csv <- sum(df.orig$AlcoholBill, na.rm = TRUE)
  
  # Print the result
  cat("Total amount spent on alcohol in CSV:", total_alcohol_amount_csv, "\n")
  
  
  # Query the database for sum of alcohol bills
  sql <- "SELECT SUM(AlcoholBill) AS TotalAlcoholBill
          FROM AlcoholOrders;"
  result <- dbGetQuery(conn, sql)
  
  # Extract the su of total food bill from the query result
  total_alcohol_amount_db <- result$TotalAlcoholBill[1]
  cat("Total amount spent on alcohol in Database:", total_alcohol_amount_db, "\n")
  
  # Compare the results from csv and the DB
  ## all.equal is used to compare floating points with small differences in the decimal points,
  if (isTRUE(all.equal(total_alcohol_amount_csv, total_alcohol_amount_db))) {
    cat("Test PASSED: The total amount spent on alcohol matches in the csv and the Database.\n\n")
  }
  else {
    cat("Test FAILED: CSV alcohol total is", total_alcohol_amount_csv,
        "but the alcohol total is", total_alcohol_amount_db, ".\n\n")
  }
  
}



# Test Sum up the total amount spent on tips --------------------------

#' This function tests the Sum up the total amount spent on tips in the
#' csv and the DB.
#'
#' @param conn the DB connection object.
#' @param df.orig the original csv containg the original data.
testSumTipsAmount <- function(conn, df.orig) {
  print("Testing sum of tips bill:")
  # Calculate the total amount spent on tips in the CSV
  total_tips_amount_csv <- sum(df.orig$TipAmount, na.rm = TRUE)
  
  # Print the result
  cat("Total amount spent on tips in CSV:", total_tips_amount_csv, "\n")
  
  
  # Query the database for sum of tips
  sql <- "SELECT SUM(TipAmount) AS TotalTips
          FROM Visits;"
  result <- dbGetQuery(conn, sql)
  
  # Extract the su of total tips from the query result
  total_tips_amount_db <- as.numeric(result$TotalTips[1])
  cat("Total amount spent on tips in Database:", total_tips_amount_db, "\n")
  
  # Compare the results from csv and the DB
  ## all.equal is used to compare floating points with small differences in the decimal points,
  if (round(total_tips_amount_csv, 2) == round(total_tips_amount_db, 2)) {
    cat("Test PASSED: The total amount spent on tips matches in the csv and the Database.\n")
  }
  else {
    cat("Test FAILED: CSV tips total is", total_tips_amount_csv,
        "but the tips total is", total_tips_amount_db, ".\n")
  }
  
}


main <- function() {
  # Get connection object
  conn = connectDB()
  
  # Read csv file from url
  df.orig <- read.csv(
    file = url("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"),
    header = T)
  
  
  # Test Unique Restaurants
  testRestaurants(conn, df.orig)
  # Test unique customers
  testCustomers(conn, df.orig)
  # Test unique Servers
  testServers(conn, df.orig)
  # Test unique Visits
  testVisits(conn, df.orig)
  # Test Sum up the total amount spent on food
  testSumFoodAmount(conn, df.orig)
  # Test Sum up the total amount spent on alcohol
  testSumAlcoholAmount(conn, df.orig)
  # Test Sum up the total amount spent on tips
  testSumTipsAmount(conn, df.orig)
  
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
