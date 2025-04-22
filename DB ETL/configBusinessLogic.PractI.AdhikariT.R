# Header ------------------------------------------------------------------

# Program: configBusinessLogic.PractI.AdhikariT.R 
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



# storeVisit --------------------------------------------------------------
#' This function creates the storeVisit procedure
#'
#' @param conn the DB connection object.
create_storeVisit <- function(conn) {
  # Drop the procedure if it already exists
  dbExecute(conn, "DROP PROCEDURE IF EXISTS storeVisit;")
  
  # Create an sql query to create a stored procedure to add the new visit.
  # We get the required parameters and add to Visits, Orders, FoodOrders
  # and AlcoholOrders table.
  # Used transaction since adding to multiple tables.
  # Used error handling block.
  sql <- "CREATE PROCEDURE storeVisit(
              IN p_restaurantID INT,
              IN p_customerID INT,
              IN p_visitDate DATE,
              IN p_partySize INT,
              IN p_foodBill DECIMAL(10, 2),
              IN p_alcoholBill DECIMAL(10, 2),
              IN p_serverEmpID INT,
              IN p_mealType INT,
              IN p_orderedAlcohol TINYINT,
              IN p_visitTime TIME
          )
          BEGIN
              -- Declare variables at the start of the block
              DECLARE visit_id INT;
    
              -- Handler to catch any SQL exceptions and roll back the transaction
              DECLARE EXIT HANDLER FOR SQLEXCEPTION
              BEGIN
                  ROLLBACK; -- Roll back the transaction in case of any error
                  SIGNAL SQLSTATE '45000'
                  SET MESSAGE_TEXT = 'Error occurred while processing storeVisit procedure.';
              END;
              
              -- Begin a transaction
              START TRANSACTION;
              
              -- Get the max VisitID and add 1 for new visitID
              SET visit_id = (SELECT MAX(VisitID) + 1 FROM Visits);
              
              -- Insert a record into FoodOrders table
              INSERT INTO FoodOrders (FoodOrderID, MealTypeID, FoodBill)
              VALUES (visit_id, p_mealType, p_foodBill);
              
              -- Insert a record into AlcoholOrders table
              INSERT INTO AlcoholOrders (AlcoholOrderID, OrderedAlcohol, AlcoholBill)
              VALUES (visit_id, p_orderedAlcohol, p_alcoholBill);
              
              -- Insert a record into Orders table
              INSERT INTO Orders (OrderID, FoodOrderID, AlcoholOrderID, TotalBill)
              VALUES (visit_id, visit_id, visit_id, p_foodBill + p_alcoholBill);
              
              -- Insert a new visit into the Visits table
              INSERT INTO Visits (VisitID, 
                                  RestaurantID, 
                                  ServerEmpID, 
                                  CustomerID, 
                                  OrderID,
                                  VisitDate, 
                                  VisitTime,
                                  PartySize)
              VALUES (visit_id, 
                      p_restaurantID, 
                      p_serverEmpID, 
                      p_customerID, 
                      visit_id,
                      p_visitDate, 
                      p_visitTime,
                      p_partySize);
              
              -- Commit the transaction if everything is successful
              COMMIT;
          END;"
  
  dbSendStatement(conn, sql);
  
  print("Created Stored Procedure")
}


# Testing storeVisit procedure --------------------------------------------

#' This function tests the storeVisit procedure
#'
#' @param conn the DB connection object.
testStoreVisit <- function(conn) {
  # Call the stored procedure
  restaurant_id <- 1
  customer_id <- 1
  visit_date <- '2025-03-03'
  party_size <- 4
  food_bill <- 150.00
  alcohol_bill <- 75.00
  server_emp_id <- 1843
  meal_type <- 1
  ordered_alcohol <- 1
  visitTime <- "'07:08:00'"
  
  # Call the stored procedure using dbExecute
  sql <- paste0("CALL storeVisit(", 
                  restaurant_id, ", ", 
                  customer_id, ", '", 
                  visit_date, "', ", 
                  party_size, ", ", 
                  food_bill, ", ", 
                  alcohol_bill, ", ", 
                  server_emp_id, ", ", 
                  meal_type, ", ", 
                  ordered_alcohol, "," ,
                  visitTime, ");")
  
  # Execute the sql query in try catch
  tryCatch({
    dbExecute(conn, sql)
    print("Stored procedure executed successfully.")
  }, error = function(e) {
    print(paste("Error executing stored procedure:", e$message))
  })
  
  # Verify the added data in the Visits table
  visits <- dbGetQuery(conn, "SELECT * FROM Visits WHERE VisitDate = '2025-03-03';")
  print(visits)
  
  # Verify the added data in the FoodOrders table
  food_orders <- dbGetQuery(conn, paste0("SELECT * FROM FoodOrders WHERE FoodOrderID IN (", 
                                         paste(visits$VisitID, collapse = ","), ");"))
  print(food_orders)
  
  # Verify the data in the AlcoholOrders table
  alcohol_orders <- dbGetQuery(conn, paste0("SELECT * FROM AlcoholOrders WHERE AlcoholOrderID IN (", 
                                            paste(visits$VisitID, collapse = ","), ");"))
  print(alcohol_orders)
  
  # Verify the data in the Orders table
  orders <- dbGetQuery(conn, paste0("SELECT * FROM Orders WHERE OrderID IN (", 
                                    paste(visits$VisitID, collapse = ","), ");"))
  print(orders)  
}



# storeNewVisit -----------------------------------------------------------

#' This function creates the storeNewVisit procedure
#'
#' @param conn the DB connection object.
create_storeNewVisit <- function(conn) {
  # Drop the procedure if it already exists
  dbExecute(conn, "DROP PROCEDURE IF EXISTS storeNewVisit;")
  
  # Create an sql query to create a stored procedure to add the new visit.
  # We get the required parameters and add to Visits, Orders, FoodOrders
  # and AlcoholOrders table, also in Restaurants, Customers and Server tables if needed.
  # Used transaction since adding to multiple tables.
  # Used error handling block.
  sql <- "CREATE PROCEDURE storeNewVisit(
              IN p_restaurantName VARCHAR(100),
              IN p_customerName VARCHAR(100),
              IN p_customerEmail VARCHAR(100),
              IN p_visitDate DATE,
              IN p_partySize INT,
              IN p_foodBill DECIMAL(10, 2),
              IN p_alcoholBill DECIMAL(10, 2),
              IN p_serverName VARCHAR(100),
              IN p_mealType INT,
              IN p_orderedAlcohol TINYINT,
              IN p_visitTime TIME
          )
          BEGIN
              -- Declare variables at the start of the block
              DECLARE restaurant_id INT;
              DECLARE customer_id INT;
              DECLARE server_id INT;
              DECLARE visit_id INT;
          
              -- Handler to catch any SQL exceptions and roll back the transaction
              DECLARE EXIT HANDLER FOR SQLEXCEPTION
              BEGIN
                  ROLLBACK; -- Roll back the transaction in case of any error
                  SIGNAL SQLSTATE '45000'
                  SET MESSAGE_TEXT = 'Error occurred while processing storeNewVisit procedure.';
              END;
          
              -- Begin a transaction
              START TRANSACTION;
          
              -- Check if the restaurant exists
              -- if it doesn't then get the next ID and insert it
              SELECT RestaurantID INTO restaurant_id
              FROM Restaurants
              WHERE RestaurantName = p_restaurantName;
          
              IF restaurant_id IS NULL THEN
                  SET restaurant_id = (SELECT MAX(RestaurantID) FROM Restaurants) + 1;
                  INSERT INTO Restaurants (RestaurantID, RestaurantName)
                  VALUES (restaurant_id, p_restaurantName);
                  
              END IF;
          
              -- Check if the customer exists
              -- if not, get the next ID and insert into Customer Table
              SELECT CustomerID INTO customer_id
              FROM Customers
              WHERE CustomerName = p_customerName 
                    AND CustomerEmail = p_customerEmail;
          
              IF customer_id IS NULL THEN
                  SET customer_id = (SELECT MAX(CustomerID) FROM Customers) + 1;
                  INSERT INTO Customers (CustomerID, CustomerName, CustomerEmail)
                  VALUES (customer_id, p_customerName, p_customerEmail);
              END IF;
          
              -- Check if the server exists
              -- if not, get the next ID insert into the Servers Table.
              SELECT ServerEmpID INTO server_id
              FROM Servers
              WHERE ServerName = p_serverName;
          
              IF server_id IS NULL THEN
                  SET server_id = (SELECT MAX(ServerEmpID) FROM Servers) + 1;
                  INSERT INTO Servers (ServerEmpID, ServerName)
                  VALUES (server_id, p_serverName);
              END IF;
          
              -- Get the last inserted VisitID add 1 to get the next VisitID
              SET visit_id = (SELECT MAX(VisitID) FROM Visits) + 1;
          
              -- Insert a record into FoodOrders table
              INSERT INTO FoodOrders (FoodOrderID, MealTypeID, FoodBill)
              VALUES (visit_id, p_mealType, p_foodBill);
          
              -- Insert a record into AlcoholOrders table
              INSERT INTO AlcoholOrders (AlcoholOrderID, OrderedAlcohol, AlcoholBill)
              VALUES (visit_id, p_orderedAlcohol, p_alcoholBill);
          
              -- Insert a record into Orders table
              INSERT INTO Orders (OrderID, FoodOrderID, AlcoholOrderID, TotalBill)
              VALUES (visit_id, visit_id, visit_id, p_foodBill + p_alcoholBill);
          
              -- Insert a new visit into the Visits table
              INSERT INTO Visits (VisitID, 
                                  RestaurantID, 
                                  ServerEmpID, 
                                  CustomerID, 
                                  OrderID,
                                  VisitDate, 
                                  VisitTime,
                                  PartySize)
              VALUES (visit_id, 
                      restaurant_id, 
                      server_id, 
                      customer_id, 
                      visit_id,
                      p_visitDate, 
                      p_visitTime,
                      p_partySize);
          
              -- Commit the transaction if everything is successful
              COMMIT;
          END;
          "
  
  dbSendStatement(conn, sql);
  
  print("Created Stored Procedure")
}


# Testing storeNewVisit procedure --------------------------------------------

#' This function tests the storeNewVisit procedure
#'
#' @param conn the DB connection object.
testStoreNewVisit <- function(conn) {
  # Define the input parameters for the procedure
  restaurantName <- "Some Restaurant"
  customerName <- "Some Name"
  customerEmail <- "some@gmail.com"
  visitDate <- "2025-03-03"
  partySize <- 4
  foodBill <- 150.00
  alcoholBill <- 75.00
  serverName <- "Some Server"
  mealType <- 2
  orderedAlcohol <- 1
  visitTime <- "18:30:00"
  
  # Create the SQL query to call the stored procedure with parameters
  sql <- sprintf(
    "CALL storeNewVisit('%s', '%s', '%s', '%s', '%d', '%.2f', '%.2f', '%s', '%d', '%d', '%s');",
    restaurantName, customerName, customerEmail, visitDate, partySize, foodBill, alcoholBill,
    serverName, mealType, orderedAlcohol, visitTime
  )
  
  # Execute the sql query in try catch
  tryCatch({
    dbExecute(conn, sql)
    print("Stored procedure executed successfully.")
  }, error = function(e) {
    print(paste("Error executing stored procedure:", e$message))
  })
  
  # Verify the added data in the Restaurants table
  restaurants <- dbGetQuery(conn, "SELECT * FROM Restaurants WHERE RestaurantName = 'Some Restaurant';")
  print(restaurants)
  # Verify the added data in the Customers table
  customers <- dbGetQuery(conn, "SELECT * FROM Customers WHERE CustomerName = 'Some Name';")
  print(customers)
  # Verify the added data in the Servers table
  servers <- dbGetQuery(conn, "SELECT * FROM Servers WHERE ServerName = 'Some Server';")
  print(servers)
  
  # Verify the added data in the Visits table
  visits <- dbGetQuery(conn, "SELECT * FROM Visits WHERE VisitDate = '2025-03-03';")
  print(visits)
  
  # Verify the added data in the FoodOrders table
  food_orders <- dbGetQuery(conn, paste0("SELECT * FROM FoodOrders WHERE FoodOrderID IN (", 
                                         paste(visits$VisitID, collapse = ","), ");"))
  print(food_orders)
  
  # Verify the data in the AlcoholOrders table
  alcohol_orders <- dbGetQuery(conn, paste0("SELECT * FROM AlcoholOrders WHERE AlcoholOrderID IN (", 
                                            paste(visits$VisitID, collapse = ","), ");"))
  print(alcohol_orders)
  
  # Verify the data in the Orders table
  orders <- dbGetQuery(conn, paste0("SELECT * FROM Orders WHERE OrderID IN (", 
                                    paste(visits$VisitID, collapse = ","), ");"))
  print(orders)  
}



main <- function() {
  # Get connection object
  conn = connectDB()
  
  # Read csv file from url
  df.orig <- read.csv(
    file = url("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"),
    header = T)
  
  # Create storeVisit Stored Procedure
  create_storeVisit(conn)
  
  # Test the storeVisit stored Procedure
  testStoreVisit(conn)
  
  # Create storeNewVisit Stored Procedure
  create_storeNewVisit(conn)
  
  # Test the storeNewVisit stored Procedure
  testStoreNewVisit(conn)
  
  # Disconnect from DB
  if (!is.null(conn) && dbIsValid(conn)) {
    dbDisconnect(conn)
  }
  
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
