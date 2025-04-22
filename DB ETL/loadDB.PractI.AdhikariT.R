# Header ------------------------------------------------------------------

# Program: loadDB.PractI.AdhikariT.R 
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



# Add data to Restaurants table in MySQLDB --------------------------------

#' This funciton populates data in the Restaurants table in 
#' the MySQL DB.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @return the unique_restaurants df.
populateRestaurants <- function(conn, df.orig) {
  # Populate Restaurants table with manual IDs
  print("Populating Restaurants table.")
  
  ## Get all the unique restaurants in the Dataframe.
  unique_restaurants <- unique(df.orig$Restaurant)
  

  ## Loop through the unique_restaurants. 
  for (i in 1:length(unique_restaurants)) {
    # Get the restaurant name
    restaurant_name <- unique_restaurants[i]
    # Assign the loop index as the RestaurantID
    restaurant_id <- i
    
    # Insert the ID and RestaurantName
    sql <- sprintf(
      "INSERT INTO Restaurants (RestaurantID, RestaurantName) VALUES (%d, '%s');",
      restaurant_id,
      gsub("'", "''", restaurant_name)
    )
    dbExecute(conn, sql)
  }

  print("Completed populating Restaurants table.")
  
  return(unique_restaurants)
}


# Add data to Servers table in MySQLDB --------------------------------

#' This funciton populates data in the Restaurants table in 
#' the MySQL DB.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @return the unique_server_ids df.
populateServers <- function(conn, df.orig) {
  # Populate Restaurants table with manual IDs
  print("Populating Servers table.")
  
  ## Get all the unique server records by ServerEmpID that are not null.
  unique_server_ids <- unique(df.orig$ServerEmpID[!is.na(df.orig$ServerEmpID)])
  
  # Loop through the unique_servers.
  for (server_id in unique_server_ids) {
    # Get the index of the first row for the current server ID in the dataframe.
    idx <- which(df.orig$ServerEmpID == server_id)[1]
    
    # Using the idx get the servername and hourlyrate.
    server_name <- gsub("'", "''", df.orig$ServerName[idx])
    hourly_rate <- df.orig$HourlyRate[idx]
    
    # Handle and store start date
    start_date <- df.orig$StartDateHired[idx]
    if (start_date == "0000-00-00") {
      start_date <- 'NULL'
    } else {
      start_date <- sprintf("'%s'", start_date)
    }
    
    # Handle and store end date
    end_date <- df.orig$EndDateHired[idx]
    if (is.na(end_date) || end_date == "" || end_date == "0000-00-00" 
        || end_date == "9999-99-99") {
      end_date <- 'NULL'
    } 
    else {
      end_date <- sprintf("'%s'", end_date)
    }
    
    # Handle and store birth date
    # Format of this date has to be change to store in DATE type column.
    birth_date <- df.orig$ServerBirthDate[idx]
    if (is.na(birth_date) || birth_date == "" || end_date == "0000-00-00" 
        || end_date == "9999-99-99") {
      birth_date <- 'NULL'
    } 
    else {
      # Convert MM/DD/YYYY to YYYY-MM-DD
      tryCatch({
        birth_date <- as.Date(birth_date, format="%m/%d/%Y")
        birth_date <- sprintf("'%s'", birth_date)
      }, error = function(e) {
        birth_date <- "NULL"
      })
    }
    
    # Handle TIN
    server_tin <- df.orig$ServerTIN[idx]
    if (is.na(server_tin) || server_tin == "") {
      server_tin <- 'NULL'
    } 
    else {
      server_tin <- sprintf("'%s'", server_tin)
    }
    
    # Create and execute the query
    sql <- sprintf(
      "INSERT INTO Servers (
                            ServerEmpID, 
                            ServerName, 
                            HourlyRate, 
                            StartDateHired, 
                            EndDateHired, 
                            ServerBirthDate, 
                            ServerTIN) 
     VALUES (%d, '%s', %f, %s, %s, %s, %s);", server_id, 
                                              server_name, 
                                              hourly_rate, 
                                              start_date, 
                                              end_date, 
                                              birth_date, 
                                              server_tin
    )
    
    # Replace 'NULL' string with actual NULL in query
    sql <- gsub("'NULL'", "NULL", sql)
    dbExecute(conn, sql)
  }
  
  print("Completed populating Servers table.")
  
  return(unique_server_ids)
}



# Add data to MealTypes table in MySQLDB --------------------------------

#' This funciton populates data in the MealTypes table in 
#' the MySQL DB.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @return the unique_mealtypes list.
populateMealTypes <- function(conn, df.orig) {
  # Populate MealTypes table with manual IDs
  print("Populating MealTypes table.")
  
  ## Get all the unique mealtypes in the Dataframe.
  unique_mealtypes <- unique(df.orig$MealType)
  
  
  ## Loop through the unique_mealtypes 
  for (i in 1:length(unique_mealtypes)) {
    # Get the mealtypes name
    meal_type <- unique_mealtypes[i]
    # Assign the loop index as the RestaurantID
    mealtype_id <- i
    
    # Insert the ID and RestaurantName
    sql <- sprintf(
      "INSERT INTO MealTypes (
                              MealTypeID, 
                              MealType) 
        VALUES (%d, '%s');",
      mealtype_id,
      gsub("'", "''", meal_type)
    )
    dbExecute(conn, sql)
  }
  
  print("Completed populating MealTypes table.")
  
  return(unique_mealtypes)
}



# Add data to Payments table in MySQLDB --------------------------------

#' This funciton populates data in the Payments table in 
#' the MySQL DB.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @return the unique_payment df.
populatePayments <- function(conn, df.orig) {
  # Populate Payments table with manual IDs
  print("Populating Payments table.")
  
  ## Get all the unique payment methods in the Dataframe.
  unique_paymentmethod <- unique(df.orig$PaymentMethod)
  
  
  ## Loop through the unique_paymentmethod 
  for (i in 1:length(unique_paymentmethod)) {
    # Get the mealtypes name
    payment_method <- unique_paymentmethod[i]
    # Assign the loop index as the RestaurantID
    payment_method_id <- i
    
    # Insert the ID and PaymentMethod
    sql <- sprintf(
      "INSERT INTO Payments (
                              PaymentID, 
                              PaymentMethod) 
        VALUES (%d, '%s');",
      payment_method_id,
      gsub("'", "''", payment_method)
    )
    dbExecute(conn, sql)
  }
  
  print("Completed populating Payments table.")
  
  return(unique_paymentmethod)
}



# Add data to Customers table in MySQLDB --------------------------------

#' This funciton populates data in the Customers table in 
#' the MySQL DB.
#' 
#' Structure of customer map, storing id for email and phone separately.
#' customer_map
#' |
#' |-- customer_email1 -> customer_id1
#' |
#' |-- customer_email2 -> customer_id2
#' |
#' |-- customer_phone1 -> customer_id1
#' |
#' |-- customer_phone2 -> customer_id2
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @return the customer_map list.
populateCustomers <- function(conn, df.orig) {
  # Populate Payments table with manual IDs
  print("Populating Customers table.")
  
  # Initialize a customers map
  customer_map <- list()
  
  # customer_id counter.
  customer_id <- 1
  
  # Loop through the df.orig to get all the customers
  for (i in 1:nrow(df.orig)) {
    # Assign customer name
    customer_name <- df.orig$CustomerName[i]
    # Assigne customer phone
    customer_phone <- df.orig$CustomerPhone[i]
    # Assign customer email
    customer_email <- df.orig$CustomerEmail[i]
    # Assign Loyalty_member
    loyalty_member <- df.orig$LoyaltyMember[i]
    
    # Skip if no customer data
    if ((is.na(customer_name) || customer_name == "") && 
        (is.na(customer_phone) || customer_phone == "") && 
        (is.na(customer_email) || customer_email == "")) {
      next
    }
    
    # Skip if customer already exists by email or phone in the customer_map.
    if (!is.na(customer_email) && customer_email != "" && !is.null(customer_map[[customer_email]])) {
      next
    }
    if (!is.na(customer_phone) && customer_phone != "" && !is.null(customer_map[[customer_phone]])) {
      next
    }
    
    # Clean values
    ## Clean Customer Name values
    if (is.na(customer_name) || customer_name == "") {
      customer_name <- "NULL"
    } else {
      customer_name <- sprintf("'%s'", gsub("'", "''", customer_name))
    }
    ## Clean Customer phone values
    if (is.na(customer_phone) || customer_phone == "") {
      customer_phone_clean <- "NULL"
    } else {
      customer_phone_clean <- sprintf("'%s'", gsub("'", "''", customer_phone))
    }
    ## Clean Customer email values
    if (is.na(customer_email) || customer_email == "") {
      customer_email_clean <- "NULL"
    } else {
      customer_email_clean <- sprintf("'%s'", gsub("'", "''", customer_email))
    }
    ## Get loyalty_member as integer 0/1.
    loyalty_member <- as.integer(loyalty_member)
    
    # Add to customer_map
    if (!is.null(customer_email) && customer_email != "") {
      customer_map[[customer_email]] <- customer_id
    }
    if (!is.null(customer_phone) && customer_phone != "") {
      customer_map[[customer_phone]] <- customer_id
    }
    
    
    # Insert the customer data into the Customers table
    sql <- sprintf(
      "INSERT INTO Customers (CustomerID, CustomerName, CustomerPhone, CustomerEmail, LoyaltyMember) 
       VALUES (%d, %s, %s, %s, %d);",
      customer_id, customer_name, customer_phone_clean, customer_email_clean, loyalty_member
    )
    sql <- gsub("'NULL'", "NULL", sql)
    dbExecute(conn, sql)
    
    # Increment the customer_id for the next customer iteration.
    customer_id <- customer_id + 1
  }
  
  print("Completed populating Customers table.")
  
  return(customer_map)
}


# Add data to AlcoholOrders table in MySQLDB --------------------------------

#' This funciton populates data in the AlcoholOrders table in 
#' the MySQL DB.
#' The function uses Batch processing for faster processing.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
populateAlcoholOrders <- function(conn, df.orig) {
  # Populate AlcoholOrders table with VisitID as the AlcoholOredrID
  print("Populating AlcoholOrders table.")
  
  # Precompute OrderedAlcohol columns (Vectorized processing)
  df.orig$orderedAlcohol <- ifelse(df.orig$orderedAlcohol == "yes", 1, 0)
  
  #Batch processing
  ## Setting batch size to use for batch process.
  batch_size = 1000
  ## Loop through the df to get the Alcohol orders in Batches.
  for (batch_start in seq(1, nrow(df.orig), by = batch_size)) {
    # Compute the batch end value
    batch_end <- min(batch_start + batch_size - 1, nrow(df.orig))
    # Get the batch of batch size from the df.orig
    batch <- df.orig[batch_start:batch_end, ]

    # Generate SQL values for the current batch
    values <- sapply(1:nrow(batch), function(i) {
      sprintf("(%d, %d, %f)", 
              batch$VisitID[i], 
              batch$orderedAlcohol[i], 
              batch$AlcoholBill[i])
    })
    
    # Combine into one SQL query to insert the values in the AlcoholOrders Table.
    sql <- sprintf(
      "INSERT INTO AlcoholOrders (AlcoholOrderID, OrderedAlcohol, AlcoholBill) VALUES %s;",
      paste(values, collapse = ", ")
    )
    dbExecute(conn, sql)
  }
  
  print("Completed populating AlcoholOrders table.")
}


# Add data to FoodOrders table in MySQLDB --------------------------------

#' This funciton populates data in the FoodOrders table in 
#' the MySQL DB.
#' The function uses Batch processing for faster processing.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @param unique_mealtype unique meal types
populateFoodOrders <- function(conn, df.orig, unique_mealtype) {
  # Populate AlcoholOrders table with VisitID as the AlcoholOredrID
  print("Populating FoodOrders table.")
  
  #Batch processing
  ## Setting batch size to use for batch process.
  batch_size = 1000
  ## Loop through the df to get the Alcohol orders in Batches.
  for (batch_start in seq(1, nrow(df.orig), by = batch_size)) {
    
    # Compute the batch end value
    batch_end <- min(batch_start + batch_size - 1, nrow(df.orig))
    # Get the batch of batch size from the df.orig
    batch <- df.orig[batch_start:batch_end, ]
    
    # Generate SQL values for the batch
    values <- sapply(1:nrow(batch), function(i) {
      # VisitID to be set as FoodOrderID.
      visit_id <- batch$VisitID[i]
      
      # Look up MealTypeID from unique_mealtype (passed in params, generated in populateMealType()).
      meal_type <- batch$MealType[i]
      meal_type_id <- which(unique_mealtype == meal_type)  # Guaranteed to exist
      
      # Get teh food bill.
      food_bill <- batch$FoodBill[i]
      
      # Format as SQL value tuple
      sprintf("(%d, %d, %f)", visit_id, meal_type_id, food_bill)
    })
    
    # Combine all values into one SQL query
    sql <- sprintf(
      "INSERT INTO FoodOrders (FoodOrderID, MealTypeID, FoodBill) VALUES %s;",
      paste(values, collapse = ", ")
    )
    
    # Execute the batch query
    dbExecute(conn, sql)
  }
  
  print("Completed populating FoodOrders table.")
}


# Add data to Orders table in MySQLDB --------------------------------

#' This funciton populates data in the Orders table in 
#' the MySQL DB.
#' The function uses Batch processing for faster processing.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
populateOrders <- function(conn, df.orig) {
  # Populate AlcoholOrders table with VisitID as the AlcoholOredrID
  print("Populating Orders table.")
  
  #Batch processing
  ## Setting batch size to use for batch process.
  batch_size = 1000
  ## Loop through the df to get the orders in Batches.
  for (batch_start in seq(1, nrow(df.orig), by = batch_size)) {
    # Compute the batch end value
    batch_end <- min(batch_start + batch_size - 1, nrow(df.orig))
    # Get the batch of batch size from the df.orig
    batch <- df.orig[batch_start:batch_end, ]
    
    # Generate SQL values for the batch
    values <- sapply(1:nrow(batch), function(i) {
      # VisitID to be set as OrderID.
      visit_id <- batch$VisitID[i]
      
      # Get the total bill.
      total_bill <- batch$FoodBill[i] + batch$AlcoholBill[i]
      
      # Format as SQL value tuple
      sprintf("(%d, %d, %d, %f)", visit_id, visit_id, visit_id, total_bill)
    })
    
    # Combine all values into one SQL query
    sql <- sprintf(
      "INSERT INTO Orders (OrderID, FoodOrderID, AlcoholOrderID, TotalBill) VALUES %s;",
      paste(values, collapse = ", ")
    )
    
    # Execute the batch query
    dbExecute(conn, sql)
  }
  
  print("Completed populating Orders table.")
}


# Add data to Visits table in MySQLDB --------------------------------

#' This funciton populates data in the Visits table in 
#' the MySQL DB.
#' The function uses Batch processing for faster processing.
#' 
#' @param conn the connection object.
#' @param df.orig the dataframe from the csv file.
#' @param unique_restaurants a list of restaurant names in the order 
#'                            of their ids in the Restaurants table.
#' @param customer_amp a map of customer email and customer phone in the key
#'                      with the ids as the values.
#' @param unique_paymentmethods a list of payment methods in ther order 
#'                              of their ids in the payments table.
populateVisits <- function(conn, df.orig, unique_restaurants, 
                         customer_map, unique_paymentmethods) {
  print("Populating Visits table.")
  
  # Precompute ServerEmpID column (Vectorized processing), making missing values as NULL.
  df.orig$ServerEmpID <- ifelse(is.na(df.orig$ServerEmpID), "NULL", df.orig$ServerEmpID)
  
  # Precompute PartySize column (Vectorized processing), making 99 as 1, since 99 is a filler value.
  df.orig$PartySize <- ifelse(is.na(df.orig$PartySize) 
                              | df.orig$PartySize == 99, 1, df.orig$PartySize)
  
  # Precompute Genders column (Vectorized processing), making empty as "u"(not necessary since no empty values).
  # repacing ' with '' is important for correct SQL syntax.
  df.orig$Genders <- ifelse(is.na(df.orig$Genders) 
                            | df.orig$Genders == "", "u", gsub("'", "''", df.orig$Genders))
  
  #Batch processing
  ## Setting batch size to use for batch process.
  batch_size = 1000
  ## Loop through the df to get the Alcohol orders in Batches.
  for (batch_start in seq(1, nrow(df.orig), by = batch_size)) {
    # Compute the batch end value
    batch_end <- min(batch_start + batch_size - 1, nrow(df.orig))
    # Get the batch of batch size from the df.orig
    batch <- df.orig[batch_start:batch_end, ]
    
    # Generate SQL values for the batch
    values <- sapply(1:nrow(batch), function(i) {
      # Get the VisitId
      visit_id <- batch$VisitID[i]
      # Get the index of the current Restaurant name in the unique restaurants list.
      restaurant_id <- which(unique_restaurants == batch$Restaurant[i])
      # Get the sereverEmpID, which was cleaned before entering the loop.
      server_emp_id <- batch$ServerEmpID[i]
      
      # Handle customer ID from customer_map by checking for email and then phone
      # and then getting the id that is in the value of the key value pair.
      customer_id <- "NULL"
      if (!is.na(batch$CustomerEmail[i]) && batch$CustomerEmail[i] != "" && 
          !is.null(customer_map[[batch$CustomerEmail[i]]])) {
        customer_id <- customer_map[[batch$CustomerEmail[i]]]
      } else if (!is.na(batch$CustomerPhone[i]) && batch$CustomerPhone[i] != "" && 
                 !is.null(customer_map[[batch$CustomerPhone[i]]])) {
        customer_id <- customer_map[[batch$CustomerPhone[i]]]
      }
      
      # Get the index of the current Restaurant name in the unique restaurants list.
      payment_id <- which(unique_paymentmethods == batch$PaymentMethod[i])
      # Get the vsit date.
      visit_date <- batch$VisitDate[i]
      # Gt the visit time.
      visit_time <- batch$VisitTime[i]
      
      # Get the PartySize, which was cleaned before entering the loop.
      party_size <- batch$PartySize[i]
      # Get the Genders, which was cleaned before entering the loop.
      genders <- batch$Genders[i]
      # Get the wait time and keep it >= 0 for constraint check.
      wait_time <- ifelse(is.na(batch$WaitTime[i]) || batch$WaitTime[i] < 0, 0, batch$WaitTime[i])
      # Get the tip amount
      tip_amount <- batch$TipAmount[i]
      # Get the DiscountApplied.
      discount_applied <- batch$DiscountApplied[i]
      
      # Format the values into a SQL tuple
      sprintf("(%d, %d, %s, %s, %d, %d, '%s', '%s', %d, '%s', %d, %.2f, %.2f)", 
              visit_id, restaurant_id, server_emp_id, customer_id, visit_id, payment_id,
              visit_date, visit_time, party_size, genders, wait_time, tip_amount, discount_applied)
    })
    
    # Combine all values into one SQL query
    sql <- sprintf(
      "INSERT INTO Visits (VisitID, RestaurantID, ServerEmpID, CustomerID, OrderID, PaymentID, 
                           VisitDate, VisitTime, PartySize, Genders, WaitTime, TipAmount, DiscountApplied) 
       VALUES %s;",
      paste(values, collapse = ", ")
    )
    sql <- gsub("'NULL'", "NULL", sql)  # Replace 'NULL' strings with actual SQL NULL values
    
    # Execute the batch sql query
    dbExecute(conn, sql)
  }
  
  print("Completed populating Visits table.")
}



main <- function() {
  # Get connection object
  conn = connectDB()
  
  # Read csv file from url
  df.orig <- read.csv(file = url("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"),
                      header = T)
  # print(names(df.orig))
  
  
  # Wrap the transaction in a tryCatch block
  tryCatch({
    # Start transaction for faster inserts
    dbExecute(conn, "START TRANSACTION;")
    
    # Populate the Restaurants table and get the
    # unique restaurants for future use.
    unique_restaurants <- populateRestaurants(conn, df.orig);
  
    # Populate the Servers table and get the
    # unique servers for future use.
    unique_server_ids <- populateServers(conn, df.orig);
    # print(unique_server_ids)
  
    # Populate the MealTypes table and get the
    # unique mealtypes for future use.
    unique_mealtypes <- populateMealTypes(conn, df.orig);
    # print(unique_mealtypes)
  
    # Populate the Payments table and get the
    # unique payment methods for future use.
    unique_paymentmethods <- populatePayments(conn, df.orig);
    # print(unique_paymentmethods)
  
    # Populate the Customers table and get the
    # customer for future use.
    customer_map <- populateCustomers(conn, df.orig);
    # print(customer_map)
  
    # Populate the AlcoholOrders table.
    # VisitID is the AlcoholOrderID
    populateAlcoholOrders(conn, df.orig);
  
    # Populate the FoodOrders table.
    # VisitID is the FoodOrderID
    populateFoodOrders(conn, df.orig, unique_mealtypes);
  
    # Populate the Orders table.
    # VisitID is the OrderID
    populateOrders(conn, df.orig);
  
    # Populate the Orders table.
    # VisitID is the OrderID
    populateVisits(conn, df.orig, unique_restaurants, customer_map, unique_paymentmethods);
    
    # Commit the transaction
    dbExecute(conn, "COMMIT;")  

  }, error = function(e) {
    # Rollback if any error occurs
    dbExecute(conn, "ROLLBACK;")
    print("Transaction rolled back due to an error:")
    print(conditionMessage(e))
  })
  
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
## Used Claude to get Batch processing real world examples in R for SQL