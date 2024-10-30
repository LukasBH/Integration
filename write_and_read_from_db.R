library(RPostgres)
library(DBI)
# Put the credentials in this script
# Never push credentials to git!! --> use .gitignore on .credentials.R
source("credentials.R")
# Function to send queries to Postgres
source("psql_queries.R")
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg1;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
"create table intg1.Department (
	department_code serial primary key,
	department_name varchar(255),
	department_location varchar(255),
	last_update timestamp(0) without time zone default current_timestamp(0)
);")
# Write rows in the new table
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
"insert into intg1.Department
	values (default, 'Computer Science', 'Aarhus C')
		  ,(default, 'Economics and Business Economics', 'Aarhus V')
		  ,(default, 'Law', 'Aarhus C')
		  ,(default, 'Medicine', 'Aarhus C');")
# Create an R dataframe
df <- data.frame(department_name = c("Education", "Chemistry"),
                 department_location = c("Aarhus N", "Aarhus C"))
# Write the dataframe to a postgres table (columns with default values are skipped)
department <- psql_append_df(cred = cred_psql_docker, 
                             schema_name = "intg1", 
                             tab_name = "department", 
                             df = df)
# Fetching rows into R
psql_select(cred = cred_psql_docker, 
            query_string = "select * from intg1.department;")

# Delete schema
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA intg1 cascade;")

#### Exercises --------
#####Exercise 3 ####
  #From R, do the following in your Postgres server (i.e. the Postgres server running in your postgres container)
    #In the ”intg1” schema, create a table called ”students” with the following columns:
      #Student_id: an autoincrementing integer column holding the primary key
      #Student_name: A varchar column where each entry can hold a maximum of 255 characters
      #department_code: An integer column
    #Insert two students into the ”students” table using the ” psql_manipulate()” function.
    #Insert two students into the ”students” table using the ”psql_append_df()” function
    #Use the ”psql_select()” function to fetch the ”students” data from Postgres and confirm that your inserts were successful

psql_manipulate(cred = cred_psql_docker,
                query_string = 
  "create table intg1.students (
    student_id serial primary key,
    student_name varchar(255),
    department_code int
  );")
#dropped table since department code was written wrong
psql_manipulate(cred = cred_psql_docker, 
                 query_string = 
                   "drop table intg1.students;")

psql_manipulate(cred = cred_psql_docker,
                query_string = 
                  "insert into intg1.students
                    values (default, 'Lukas', 1),
                            (default, 'Hans', 2);")
df2 <- data.frame(student_name = c("Jon", "Kasper"),
                  department_code = c(3, 4))
students <- psql_append_df(cred = cred_psql_docker,
                           schema_name = "intg1",
                           tab_name = "students",
                           df = df2) 
psql_select(cred = cred_psql_docker,
            query_string = 
              "select * from intg1.students;")



