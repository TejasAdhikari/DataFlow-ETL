# Header ------------------------------------------------------------------

# Program: loadAnalyticsDB.PractII.AdhikariT.R
# Name: Tejas Adhikari
# Semester: Spring 2025

# Header end --------------------------------------------------------------


# Install packages --------------------------------------------------------
# Install RSQLite package if not installed already
# This makes the code portable
if("RSQLite" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RSQLite")
}
# Load RSQLite Library
library("RSQLite")

# Install DBI package if not installed already
# This makes the code portable
if("DBI" %in% rownames(installed.packages()) == FALSE) {
  install.packages("DBI")
}
library(DBI)




# Connect to SQLite and MySQL DBs ----------------------------------------

#' This function helps connect to the dbfiles in the SQLite and
#' MySQL Databases.
#' 
#' @return returns a list of 3 connection objects.
#'         1. Connection to MySQL DB
#'         2. Connection to film DB
#'         3. Connection to music DB
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
  
  
  
  # Connect to the SQLite databases
  conn1 <- dbConnect(RSQLite::SQLite(), "film-sales.db")
  conn2 <- dbConnect(RSQLite::SQLite(), "music-sales.db")
  
  return(list(conn = conn, conn1 = conn1, conn2 = conn2))
}


# Add data to CountryDimension table in MySQLDB --------------------------------

#' This funciton populates data in the CountryDimension table in 
#' the MySQL DB.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
populateCountries <- function(connMySQL, connFilm, connMusic) {
  # Populate CountryDimension table with manual IDs
  print("Populating CountryDimension table.")
  
  ## Get all the unique countries from Film in a Dataframe.
  sql <- "SELECT DISTINCT country
          FROM country;"
  dfFilm <- dbGetQuery(connFilm, sql)
  # print(head(df))
  
  ## Get all the unique countries from Music in a Dataframe.
  sql <- "SELECT 
            REPLACE(country, 'USA', 'United States') AS country
          FROM customers;"
  dfMusic <- dbGetQuery(connMusic, sql)
  # print(head(df))

  # Ensure the column names match between dfFilm and dfMusic
  colnames(dfMusic) <- colnames(dfFilm)
  # Combine the dataframes and remove duplicates
  df <- unique(rbind(dfFilm, dfMusic))

  ## Loop through the countries. 
  for (i in 1:nrow(df)) {
    # Get the restaurant name
    country <- df$country[i]
    
    # Insert the country
    sql <- sprintf(
      "INSERT IGNORE INTO CountryDimension (Country) VALUES ('%s') ;",
      gsub("'", "''", country)
    )
    dbExecute(connMySQL, sql)
  }
  
  print("Completed populating CountryDimension table.")
}



# Add data to PeriodDimension table in MySQLDB --------------------------------

#' This funciton populates data in the PeriodDimension table in 
#' the MySQL DB.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
populatePeriods <- function(connMySQL, connFilm, connMusic) {
  # Populate PeriodDimension table with manual IDs
  print("Populating PeriodDimension table.")
  
  ## Get all the unique periods from Film in a Dataframe.
  sql <- "SELECT 
            strftime('%m', payment_date) AS Month,
            strftime('%Y', payment_date) AS Year,
            CASE 
                WHEN strftime('%m', payment_date) IN ('01', '02', '03') THEN 1
                WHEN strftime('%m', payment_date) IN ('04', '05', '06') THEN 2
                WHEN strftime('%m', payment_date) IN ('07', '08', '09') THEN 3
                WHEN strftime('%m', payment_date) IN ('10', '11', '12') THEN 4
            END AS Quarter
          FROM payment
          GROUP BY Year, Month;"
  dfFilm <- dbGetQuery(connFilm, sql)
  # print(head(dfFilm))
  
  
  ## Get all the unique periods from Music in a Dataframe.
  sql <- "SELECT 
            strftime('%m', InvoiceDate) AS Month,
            strftime('%Y', InvoiceDate) AS Year,
            CASE 
                WHEN strftime('%m', InvoiceDate) IN ('01', '02', '03') THEN 1
                WHEN strftime('%m', InvoiceDate) IN ('04', '05', '06') THEN 2
                WHEN strftime('%m', InvoiceDate) IN ('07', '08', '09') THEN 3
                WHEN strftime('%m', InvoiceDate) IN ('10', '11', '12') THEN 4
            END AS Quarter
          FROM invoices
          GROUP BY Year, Month;"
  dfMusic <- dbGetQuery(connMusic, sql)
  # print(head(dfMusic))


  # Ensure the column names match between dfFilm and dfMusic
  colnames(dfMusic) <- colnames(dfFilm)
  # Combine the dataframes and remove duplicates
  df <- unique(rbind(dfFilm, dfMusic))
  
  ## Loop through the countries.
  for (i in 1:nrow(df)) {
    # Get the restaurant name
    month <- df$Month[i]
    year <- df$Year[i]
    quarter <- df$Quarter[i]
    
    # print(paste0(month, " ", year, " ", quarter))

    # Insert the country
    sql <- sprintf(
      "INSERT IGNORE INTO PeriodDimension (
                                            Month, 
                                            Year, 
                                            Quarter) 
                          VALUES ('%s', '%s', '%s') ;",
      month, year, quarter
    )
    dbExecute(connMySQL, sql)
  }

  print("Completed populating PeriodDimension table.")
}




# Add data to SalesFact table in MySQLDB --------------------------------

#' This funciton populates data in the SalesFact table in 
#' the MySQL DB.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
populateSalesFacts <- function(connMySQL, connFilm, connMusic) {
  # Populate SalesFact table with manual IDs
  print("Populating SalesFact table.")
  
  ## Get all the unique periods from Film in a Dataframe.
  sql <- "SELECT 
            c.country,
            strftime('%m', payment_date) AS Month,
            strftime('%Y', payment_date) AS Year,
            CASE 
                WHEN strftime('%m', payment_date) IN ('01', '02', '03') THEN 1
                WHEN strftime('%m', payment_date) IN ('04', '05', '06') THEN 2
                WHEN strftime('%m', payment_date) IN ('07', '08', '09') THEN 3
                WHEN strftime('%m', payment_date) IN ('10', '11', '12') THEN 4
            END AS Quarter,
            SUM(p.amount) AS MonthlyRevenue,
            COUNT(*) AS MonthlyUnitsSold,
            'Film' AS Type
          FROM payment p
          INNER JOIN customer cu ON p.customer_id = cu.customer_id
          INNER JOIN address a ON cu.address_id = a.address_id
          INNER JOIN city ci ON a.city_id = ci.city_id
          INNER JOIN country c ON ci.country_id = c.country_id
          GROUP BY c.country, Year, Month;"
  dfFilm <- dbGetQuery(connFilm, sql)
  # print(head(dfFilm))
  
  
  ## Get all the unique periods from Music in a Dataframe.
  sql <- "SELECT 
            c.country,
            strftime('%m', InvoiceDate) AS Month,
            strftime('%Y', InvoiceDate) AS Year,
            CASE 
                WHEN strftime('%m', InvoiceDate) IN ('01', '02', '03') THEN 1
                WHEN strftime('%m', InvoiceDate) IN ('04', '05', '06') THEN 2
                WHEN strftime('%m', InvoiceDate) IN ('07', '08', '09') THEN 3
                WHEN strftime('%m', InvoiceDate) IN ('10', '11', '12') THEN 4
            END AS Quarter,
            SUM(i.Total) AS MonthlyRevenue,
            COUNT(ii.InvoiceLineId) AS MonthlyUnitsSold,
            'Music' AS Type
          FROM invoices i
          INNER JOIN customers c ON i.CustomerId = c.CustomerId
          INNER JOIN invoice_items ii ON i.InvoiceId = ii.InvoiceId
          GROUP BY c.Country, Year, Month;"
  dfMusic <- dbGetQuery(connMusic, sql)
  # print(head(dfMusic))


  # Ensure the column names match between dfFilm and dfMusic
  colnames(dfMusic) <- colnames(dfFilm)
  # Combine the dataframes and remove duplicates
  df <- unique(rbind(dfFilm, dfMusic))
  # print(head(df))

  ## Loop through the countries.
  for (i in 1:nrow(df)) {
    # Get the restaurant name
    month <- df$Month[i]
    year <- df$Year[i]
    quarter <- df$Quarter[i]
    country <- df$country[i]
    if (country == "USA") {
      country <- "United States"
    }
    

    # print(paste0(month, " ", year, " ", quarter))

    # Get the periodId
    sql <- sprintf("SELECT id
                    FROM PeriodDimension
                    WHERE Month = '%s' AND Year = '%s' AND QUARTER = '%s';",
      month, year, quarter
    )
    periodId <- dbGetQuery(connMySQL, sql)
    
    # Get the countryId
    sql <- sprintf("SELECT id
                    FROM CountryDimension
                    WHERE country = '%s';",
                   country
    )
    countryId <- dbGetQuery(connMySQL, sql)

    # print(countryId)

    # Insert the country
    sql <- sprintf(
      "INSERT IGNORE INTO SalesFacts (
                                      MonthlyRevenue,
                                      MonthlyUnitsSold,
                                      PeriodId,
                                      CountryId,
                                      Type)
                          VALUES ('%f', '%d', '%d', '%d', '%s') ;",
      df$MonthlyRevenue[i],
      df$MonthlyUnitsSold[i],
      periodId$id,
      countryId$id,
      df$Type[i]
    )
    dbExecute(connMySQL, sql)
  }
  
  print("Completed populating SalesFact table.")
}




# Add data to CustomerFacts table in MySQLDB --------------------------------

#' This funciton populates data in the CustomerFacts table in 
#' the MySQL DB.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
populateCustomerFacts <- function(connMySQL, connFilm, connMusic) {
  # Populate CustomerFacts table with manual IDs
  print("Populating CustomerFacts table.")
  
  ## Get all the data from Film in a Dataframe.
  sql <- "SELECT 
            c.country, 
            COUNT(DISTINCT cu.customer_id) AS CustomerCount,
            'Film' AS Type
          FROM customer cu
          INNER JOIN address a ON cu.address_id = a.address_id
          INNER JOIN city ci ON a.city_id = ci.city_id
          INNER JOIN country c ON ci.country_id = c.country_id
          GROUP BY c.country;"
  dfFilm <- dbGetQuery(connFilm, sql)
  # print(head(dfFilm))
  
  
  ## Get all the data from Music in a Dataframe.
  sql <- "SELECT 
            Country, 
            COUNT(DISTINCT CustomerId) AS CustomerCount,
            'Music' AS Type
          FROM customers
          GROUP BY Country;"
  dfMusic <- dbGetQuery(connMusic, sql)
  # print(head(dfMusic))
  
  
  # Ensure the column names match between dfFilm and dfMusic
  colnames(dfMusic) <- colnames(dfFilm)
  # Combine the dataframes and remove duplicates
  df <- unique(rbind(dfFilm, dfMusic))
  # print(head(df))
  
  ## Loop through the countries.
  for (i in 1:nrow(df)) {
    # Get the restaurant name
    country <- df$country[i]
    if (country == "USA") {
      country <- "United States"
    }
    
    CustomerCount <- df$CustomerCount[i]
    Type <- df$Type[i]


    # Get the countryId
    sql <- sprintf("SELECT id
                    FROM CountryDimension
                    WHERE country = '%s';",
                   country
    )
    countryId <- dbGetQuery(connMySQL, sql)
    
    
    # Insert the country
    sql <- sprintf(
      "INSERT IGNORE INTO CustomerFacts (
                                      CustomerCount, 
                                      CountryId, 
                                      Type)
                          VALUES ('%d', '%d', '%s');",
      CustomerCount,
      countryId$id,
      df$Type[i]
    )
    dbExecute(connMySQL, sql)
  }
  
  print("Completed populating CustomerFacts table.")
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




# Test SalesFacts --------------------------------------------------------

#' This function tests the revenue, unit counts, period and Country in the DBs.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
testSalesFacts <- function(connMySQL, connFilm, connMusic) {
  cat("\n\n")
  print("Testing SalesFacts table:")
  
  
  # Query the SQLite database for the required data from Film db.
  sql <- "SELECT 
            c.country AS Country,
            CAST(strftime('%m', payment_date) AS INTEGER) AS Month,
            CAST(strftime('%Y', payment_date) AS INTEGER) AS Year,
            CASE 
                WHEN strftime('%m', payment_date) IN ('01', '02', '03') THEN 1
                WHEN strftime('%m', payment_date) IN ('04', '05', '06') THEN 2
                WHEN strftime('%m', payment_date) IN ('07', '08', '09') THEN 3
                WHEN strftime('%m', payment_date) IN ('10', '11', '12') THEN 4
            END AS Quarter,
            SUM(p.amount) AS MonthlyRevenue,
            COUNT(*) AS MonthlyUnitsSold,
            'Film' AS Type
          FROM payment p
          INNER JOIN customer cu ON p.customer_id = cu.customer_id
          INNER JOIN address a ON cu.address_id = a.address_id
          INNER JOIN city ci ON a.city_id = ci.city_id
          INNER JOIN country c ON ci.country_id = c.country_id
          GROUP BY c.country, Year, Month;"
  dfFilm <- dbGetQuery(connFilm, sql)
  # SELECT the same data from the MySQL db for film.
  sql <- "SELECT 
            cd.Country, 
            pd.Month, 
            pd.Year, 
            pd.Quarter,
            sf.MonthlyRevenue, 
            sf.MonthlyUnitsSold,
            sf.Type
          FROM SalesFacts sf
          INNER JOIN CountryDimension cd ON sf.CountryId = cd.id
          INNER JOIN PeriodDimension pd ON sf.PeriodId = pd.id
          WHERE sf.Type = 'Film';"
  mdfFilm <- suppressWarnings(dbGetQuery(connMySQL, sql))
  
  
  # Query the SQLite database for the required data from Music db.
  sql <- "SELECT 
            REPLACE(c.country, 'USA', 'United States') AS Country,
            CAST(strftime('%m', i.InvoiceDate) AS INTEGER) AS Month,
            CAST(strftime('%Y', i.InvoiceDate) AS INTEGER) AS Year,
            CASE 
                WHEN strftime('%m', i.InvoiceDate) IN ('01', '02', '03') THEN 1
                WHEN strftime('%m', i.InvoiceDate) IN ('04', '05', '06') THEN 2
                WHEN strftime('%m', i.InvoiceDate) IN ('07', '08', '09') THEN 3
                WHEN strftime('%m', i.InvoiceDate) IN ('10', '11', '12') THEN 4
            END AS Quarter,
            SUM(i.Total) AS MonthlyRevenue,
            COUNT(ii.InvoiceLineId) AS MonthlyUnitsSold,
            'Music' AS Type
          FROM invoices i
          INNER JOIN customers c ON i.CustomerId = c.CustomerId
          INNER JOIN invoice_items ii ON i.InvoiceId = ii.InvoiceId
          GROUP BY c.Country, Year, Month;"
  dfMusic <- dbGetQuery(connMusic, sql)
  # SELECT the =same data from the MySQL db for music.
  sql <- "SELECT 
            cd.Country, 
            pd.Month, 
            pd.Year, 
            pd.Quarter,
            sf.MonthlyRevenue, 
            sf.MonthlyUnitsSold,
            sf.Type
          FROM SalesFacts sf
          INNER JOIN CountryDimension cd ON sf.CountryId = cd.id
          INNER JOIN PeriodDimension pd ON sf.PeriodId = pd.id
          WHERE sf.Type = 'Music';"
  mdfMusic <- suppressWarnings(dbGetQuery(connMySQL, sql))
  
  
  # Compare the results from SQLite and MySQL DB for the Film data
  if (isTRUE(all.equal(dfFilm, mdfFilm, tolerance = 1e-10))) {
    cat("Test PASSED: The data from SQLite and MySQL DB for Films match.\n")
  }
  else {
    cat("Test FAILED: SQLite and MySQL data do not match.\n")
  }
  
  # Compare the results from SQLite and MySQL DB for the Music data
  if (isTRUE(all.equal(dfMusic, mdfMusic, tolerance = 1e-10))) {
    cat("Test PASSED: The data from SQLite and MySQL DB for Music match.\n\n")
  }
  else {
    cat("Test FAILED: SQLite and MySQL data do not match.\n\n")
  }
}



# Test CustomerFacts --------------------------------------------------------

#' This function tests the revenue, unit counts, period and Country in the DBs.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
testCustomerFacts <- function(connMySQL, connFilm, connMusic) {
  print("Testing CustomerFacts table:")
  
  
  # Query the SQLite database for the required data from Film db.
  sql <- "SELECT 
            c.country AS Country, 
            COUNT(DISTINCT cu.customer_id) AS CustomerCount,
            'Film' AS Type
          FROM customer cu
          INNER JOIN address a ON cu.address_id = a.address_id
          INNER JOIN city ci ON a.city_id = ci.city_id
          INNER JOIN country c ON ci.country_id = c.country_id
          GROUP BY c.country;"
  dfFilm <- dbGetQuery(connFilm, sql)
  # SELECT the same data from the MySQL db for film.
  sql <- "SELECT 
            cd.Country, 
            cf.CustomerCount,
            cf.Type
          FROM CustomerFacts cf
          INNER JOIN CountryDimension cd ON cf.CountryId = cd.id
          WHERE cf.Type = 'Film';"
  mdfFilm <- suppressWarnings(dbGetQuery(connMySQL, sql))
  
  
  # Query the SQLite database for the required data from Music db.
  sql <- "SELECT 
            REPLACE(Country, 'USA', 'United States') AS Country, 
            COUNT(DISTINCT CustomerId) AS CustomerCount,
            'Music' AS Type
          FROM customers
          GROUP BY Country;"
  dfMusic <- dbGetQuery(connMusic, sql)
  # SELECT the =same data from the MySQL db for music.
  sql <- "SELECT 
            cd.Country, 
            cf.CustomerCount,
            cf.Type
          FROM CustomerFacts cf
          INNER JOIN CountryDimension cd ON cf.CountryId = cd.id
          WHERE cf.Type = 'Music';"
  mdfMusic <- suppressWarnings(dbGetQuery(connMySQL, sql))


  # Compare the results from SQLite and MySQL DB for the Film data
  if (isTRUE(all.equal(dfFilm, mdfFilm, tolerance = 1e-10))) {
    cat("Test PASSED: The data from SQLite and MySQL DB for Films match.\n")
  }
  else {
    cat("Test FAILED: SQLite and MySQL data do not match.\n")
  }
  
  # Compare the results from SQLite and MySQL DB for the Music data
  if (isTRUE(all.equal(dfMusic, mdfMusic, tolerance = 1e-10))) {
    cat("Test PASSED: The data from SQLite and MySQL DB for Music match.\n\n")
  }
  else {
    cat("Test FAILED: SQLite and MySQL data do not match.\n\n")
  }
}




# Test number of customers  --------------------------------------------

#' This function tests the number of customers in the US for both 
#' films and music in the DBs.
#' 
#' @param connMySQL the connection object for MySQL db.
#' @param connFilm the connection object for Film SQLite db.
#' @param connMusic the connection object for Music SQLite db.
testNumberOfCustomersUS <- function(connMySQL, connFilm, connMusic) {
  print("Testing number of customers in the US:")
  
  
  # Query the SQLite database for the required data from Film db.
  sql <- "SELECT 
            COUNT(DISTINCT cu.customer_id) AS CustomerCount,
            'Film' AS Type
          FROM customer cu
          INNER JOIN address a ON cu.address_id = a.address_id
          INNER JOIN city ci ON a.city_id = ci.city_id
          INNER JOIN country c ON ci.country_id = c.country_id
          WHERE c.country = 'United States';"
  dfFilm <- dbGetQuery(connFilm, sql)
  # SELECT the same data from the MySQL db for film.
  sql <- "SELECT 
            cf.CustomerCount,
            cf.Type
          FROM CustomerFacts cf
          INNER JOIN CountryDimension cd ON cf.CountryId = cd.id
          WHERE cf.Type = 'Film' AND cd.Country = 'United States';"
  mdfFilm <- suppressWarnings(dbGetQuery(connMySQL, sql))
  
  
  # Query the SQLite database for the required data from Music db.
  sql <- "SELECT
            COUNT(DISTINCT CustomerId) AS CustomerCount,
            'Music' AS Type
          FROM customers
          WHERE Country = 'USA';"
  dfMusic <- dbGetQuery(connMusic, sql)
  # print(dfMusic)
  # SELECT the =same data from the MySQL db for music.
  sql <- "SELECT
            cf.CustomerCount,
            cf.Type
          FROM CustomerFacts cf
          INNER JOIN CountryDimension cd ON cf.CountryId = cd.id
          WHERE cf.Type = 'Music' AND cd.Country = 'United States';"
  mdfMusic <- suppressWarnings(dbGetQuery(connMySQL, sql))


  # Compare the results from SQLite and MySQL DB for the Film data
  if (isTRUE(all.equal(dfFilm, mdfFilm, tolerance = 1e-10))) {
    cat("Test PASSED: The data from SQLite and MySQL DB for Films match.\n")
  }
  else {
    cat("Test FAILED: SQLite and MySQL data do not match.\n")
  }
  # Print the counts for both
  cat("Count from SQLite DB for films: ", dfFilm$CustomerCount, "\n")
  cat("Count from MySQL DB for films: ", mdfFilm$CustomerCount, "\n")

  
  # Compare the results from SQLite and MySQL DB for the Music data
  if (isTRUE(all.equal(dfMusic, mdfMusic, tolerance = 1e-10))) {
    cat("Test PASSED: The data from SQLite and MySQL DB for Music match.\n")
  }
  else {
    cat("Test FAILED: SQLite and MySQL data do not match.\n")
  }
  # Print the counts for both
  cat("Count from SQLite DB for music: ", dfMusic$CustomerCount, "\n")
  cat("Count from MySQL DB for music: ", mdfMusic$CustomerCount, "\n\n")
}





main <- function() {
  tryCatch({
    # Get all the connection objects and separate them in different variables.
    connections <- connectDB()
    
    connFilm <- connections$conn1
    connMusic <- connections$conn2
    connMySQL <- connections$conn
    deleteFromTables(connMySQL)

    # Mark start time.
    start_time <- Sys.time()

    # populateCountries in the country dimension table.
    populateCountries(connMySQL, connFilm, connMusic)
    # populatePeriods in the Period dimension table.
    populatePeriods(connMySQL, connFilm, connMusic)
    # populateSalesFacts in the SalesFacts table.
    populateSalesFacts(connMySQL, connFilm, connMusic)
    # populateCustpmerFacts in the CustpmerFacts table.
    populateCustomerFacts(connMySQL, connFilm, connMusic)

    # Mark end time.
    end_time <- Sys.time()
    # Calculate the total time taken.
    time_taken <- end_time - start_time
    print(paste0("Time taken to load the data into the Fact and Dimension tables: ", round(time_taken, 2)))

    # Test the loaded data with the data in SQLite
    testSalesFacts(connMySQL, connFilm, connMusic)
    testCustomerFacts(connMySQL, connFilm, connMusic)
    testNumberOfCustomersUS(connMySQL, connFilm, connMusic)
  }, 
  finally = {
    # Disconnect from all the dbs 
    if (exists("connMySQL") && !is.null(connMySQL)) {
      dbDisconnect(connMySQL)
    }
    if (exists("connFilm") && !is.null(connFilm)) {
      dbDisconnect(connFilm)
    }
    if (exists("connMusic") && !is.null(connMusic)) {
      dbDisconnect(connMusic)
    }
    
    cat("All connections have been closed.\n")
  })
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