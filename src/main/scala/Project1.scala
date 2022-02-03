import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1{//begin of object
    def main(args: Array[String]): Unit = {
        // This block of code is all necessary for spark/hive/hadoop
        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // change if winutils.exe is in a different bin folder
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1")    // Change to whatever app name you want
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._

        //This block to connect to mySQL
        val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/Payroll" // Modify for whatever port you are running your DB on
        val username = "root"
        val password = "4370335s" // Update to include your password
        var connection:Connection = null 
        val userInput=new Scanner(System.in)
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
         print("===========================================")
        print("| Welcome To Lending Club Loan Data!|")
        println("===========================================")
        // Method to check login credentials
        // insertCovidData(hiveCtx)
        val adminCheck = login(connection)
        if (adminCheck) {
            print("===========================================")
            print("| Welcome Admin! Loading in data...|")
            println("===========================================")
            println("Option 1: Add a new user ")
            println("Option 2: Delete a user")
            println("Option 3: Average loan amount by state ")
            println("Option 4:Total loan amount by state ")
            println("Option 5:Top 20 loaners, Employment Type and loan purpose ")
            println("Option 6:Job Titles that have loan more than 20000")
            println("Option 7:State with Highest number of Deliquencies and Loan status ")
            println("Option 8: Employement type with Fully paid Loan status")
            println("Option 9: Enter 'q' to quit")
            
            var option1=userInput.next() 
              userInput.nextLine()
            //perfecting the option the user choose
             while(option1!="q"){
              
             if(option1=="1"){ 

            println("What is new user name?") 
            var newUserName=userInput.nextLine() 
             newUserName="'"+newUserName+"'"
            println("What is the temporary password?")  
            var newUserPassword=userInput.nextLine()  
            newUserPassword="'"+newUserPassword+"'"

            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            val statement = connection.createStatement()
            val updateUser= statement.executeUpdate("Insert into user_accounts(user_name, password) values("+newUserName+" ,"+newUserPassword+")")

             }


             if(option1=="2"){
             println("Please enter username you want to delete")   
             var deleteUser=userInput.nextLine()    
             deleteUser="'"+deleteUser+"'"
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            val statement2 = connection.createStatement()
            val deleteUserInfo= statement2.executeUpdate("delete from user_accounts where user_name="+deleteUser+" ")
    }



             if(option1=="3"){averageLoanbyState(hiveCtx:HiveContext)}
             if(option1=="4"){totalLoanAmount(hiveCtx:HiveContext)}
             if(option1=="5"){ top25LoansPurposes(hiveCtx:HiveContext)}
             if(option1=="6"){jobWithLoanMoreThan20K(hiveCtx:HiveContext)}
             if(option1=="7"){stateWithHighDeliq(hiveCtx:HiveContext)}
             if(option1=="8"){numOfDebtConsolidation(hiveCtx:HiveContext)}
             
            
            
            println("===========================================")
            println("===========================================")
            println("|Do you want to continue or press q to quit?|")
            println("===========================================")
            println("===========================================")
            println("Option 1: Add a new user ")
            println("Option 2: Delete a user")
            println("Option 3: Average loan amount by state ")
            println("Option 4:Total loan amount by state ")
            println("Option 5:Top 20 loaners, Employment Type and loan purpose ")
            println("Option 6:Job Titles that have loan more than 20000")
            println("Option 7:State with Highest number of Deliquencies and Loan status ")
            println("Option 8: Employement type with Fully paid Loan status")
            println("Option 9: Enter 'q' to quit")
           

            option1=userInput.next() 
            userInput.nextLine() 
          
            
             }

            val adminCheck2 = login(connection)

             
        } else {
            println("===========================================")
            println("| Welcome User! Loading in data... |")
            println("===========================================")

              
            println("Option 1: Update password ")
            println("Option 2: Average loan amount by state ")
            println("Option 3:Total loan amount by state ")
            println("Option 4:Top 20 loaners, Employment Type and loan purpose ")
            println("Option 5:Job Titles that have loan more than 20000")
            println("Option 6:State with Highest number of Deliquencies and Loan status ")
            println("Option 7: Employement type with Fully paid Loan status")
            println("Option 8: Enter 'q' to quit")
            var option2=userInput.next()
               userInput.nextLine()
           while(option2!="q"){

           
             if(option2=="1"){
             println("Please enter username password you want to update")   
             var updateUser=userInput.nextLine()    
             updateUser="'"+updateUser+"'"
             println("Please enter your new password")   
             var newPassword=userInput.nextLine() 
             newPassword="'"+newPassword+"'"   
             Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            val statement2 = connection.createStatement()
            val updateUserInfo= statement2.executeUpdate("update user_accounts set password="+newPassword+" where user_name="+updateUser+" ")
            println("Congrajulations your password is successfully updated")

             }

            
             if(option2=="2"){averageLoanbyState(hiveCtx:HiveContext)}
             if(option2=="3"){totalLoanAmount(hiveCtx:HiveContext)}
             if(option2=="4"){ top25LoansPurposes(hiveCtx:HiveContext)}
             if(option2=="5"){jobWithLoanMoreThan20K(hiveCtx:HiveContext)}
             if(option2=="6"){stateWithHighDeliq(hiveCtx:HiveContext)}
             if(option2=="7"){numOfDebtConsolidation(hiveCtx:HiveContext)}

              
            println("===========================================")
            println("===========================================")
            println("|Do you want to continue or press q to quit?|")
            println("===========================================")
            println("===========================================")
           
            println("Option 1: Update password ")
            println("Option 2: Average loan amount by state ")
            println("Option 3:Total loan amount by state ")
            println("Option 4:Top 20 loaners, Employment Type and loan purpose ")
            println("Option 5:Job Titles that have loan more than 20000")
            println("Option 6:State with Highest number of Deliquencies and Loan status ")
            println("Option 7: Employement type with Fully paid Loan status")
            println("Option 8: Enter 'q' to quit")
           
               option2=userInput.next()
               userInput.nextLine()
           
           }




        }
        

        // Run method to insert Covid data. Only needs to be ran initially, then table data1 will be persisted.
         //insertLoanData(hiveCtx)


        /*
        * Here is where I would ask the user for input on what queries they would like to run, as well as
        * method calls to run those queries. An example is below, top10DeathRates(hiveCtx) 
        * 
        */

       // top10DeathRates(hiveCtx)

        sc.stop() // Necessary to close cleanly. Otherwise, spark will continue to run and run into problems.
    }

    // This method checks to see if a user-inputted username/password combo is part of a mySQL table.
    // Returns true if admin, false if basic user, gets stuckl in a loop until correct combo is inputted (FIX)
    def login(connection: Connection): Boolean = {
        
        while (true) {
            val statement = connection.createStatement()
            val statement2 = connection.createStatement()
            println("Enter username: ")
            var scanner = new Scanner(System.in)
            var username = scanner.nextLine().trim()

            println("Enter password: ")
            var password = scanner.nextLine().trim()
            val resultSet = statement.executeQuery("SELECT COUNT(*) FROM admin_accounts WHERE user_name='"+username+"' AND password='"+password+"';")
            while ( resultSet.next() ) {
                if (resultSet.getString(1) == "1") {
                    return true;
                }
            }

            val resultSet2 = statement2.executeQuery("SELECT COUNT(*) FROM user_accounts WHERE user_name='"+username+"' AND password='"+password+"';")
            while ( resultSet2.next() ) {
                if (resultSet2.getString(1) == "1") {
                    return false;
                }
            }

            println("Username/password combo not found. Try again!")
        }
        return false
    }

    def insertLoanData(hiveCtx:HiveContext): Unit = {
                //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/covid_19_data.txt' OVERWRITE INTO TABLE data1")
        //hiveCtx.sql("INSERT INTO data1 VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

        // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
        // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can 
        // then be 
        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/loans_full_schema.csv")
        output.limit(15).show() // Prints out the first 15 lines of the dataframe

        // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries. 

        // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
        // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this 
        // code as well as the creation of output will not be necessary.
        //hiveCtx.sql("drop table data1")
        //hiveCtx.sql("drop view data2")
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1 (emp_title STRING,emp_length INT,state CHAR(2),homeownership STRING,annual_income INT,verified_income STRING,delinq_2y INT,months_since_last_delinq INT,num_historical_failed_to_pay INT,total_debit_limit INT,account_never_delinq_percent DECIMAL,public_record_bankrupt INT,loan_purpose STRING,application_type STRING,loan_amount INT,term INT,interest_rate DECIMAL,installment DECIMAL,loan_status STRING,balance DECIMAL,paid_total DECIMAL,paid_principal DECIMAL,paid_interest DECIMAL,paid_late_fees DECIMAL)")
        hiveCtx.sql("INSERT INTO data1 SELECT * FROM temp_data")
        //hiveCtx.sql("create view  Data2 as select emp_title,emp_length,homeownership,annual_income,verified_income,delinq_2y,months_since_last_delinq,num_historical_failed_to_pay,total_debit_limit,account_never_delinq_percent,public_record_bankrupt,loan_purpose,application_type,loan_amount,term,interest_rate,installment,loan_status,balance,paid_total,paid_principal,paid_interest,paid_late_fees,state from data1")
            hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
            hiveCtx.sql("SET hive.enforce.bucketing=false")
            hiveCtx.sql("SET hive.enforce.sorting=false")
        hiveCtx.sql("create table IF NOT EXISTS Partition_Data2 (emp_title STRING,emp_length INT,homeownership STRING,annual_income INT,verified_income STRING,delinq_2y INT,months_since_last_delinq INT,num_historical_failed_to_pay INT,total_debit_limit INT,account_never_delinq_percent DECIMAL,public_record_bankrupt INT,loan_purpose STRING,application_type STRING,loan_amount INT,term INT,interest_rate DECIMAL,installment DECIMAL,loan_status STRING,balance DECIMAL,paid_total DECIMAL,paid_principal DECIMAL,paid_interest DECIMAL,paid_late_fees DECIMAL) PARTITIONED BY (state CHAR(2)) CLUSTERED BY (loan_amount) INTO 4 BUCKETS row format delimited fields terminated by ',' stored as textfile TBLPROPERTIES(\"skip.header.line.count\"=\"1\")")
        hiveCtx.sql("INSERT INTO Partition_Data2 SELECT * FROM data2")
        // To query the data1 table. When we make a query, the result set ius stored using a dataframe. In order to print to the console, 
        // we can use the .show() method.
        val summary = hiveCtx.sql("SELECT * FROM data1 LIMIT 10")
        summary.show()
    }
/*
    def top10DeathRates(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("SELECT Province_State State, MAX(deaths)/MAX(confirmed) Death_Rate FROM data1 WHERE country_region='US' AND Province_State NOT LIKE '%,%' GROUP BY State ORDER BY Death_Rate DESC LIMIT 10")
        result.show()
        result.write.csv("results/top10DeathRates")
    } **/

def averageLoanbyState(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("select state, (floor(AVG(loan_amount))) as totalAverageLoanAmount from Partition_Data2  group by state ")
        result.show()
        //result.write.csv("results/averageLoanbyState")
    }

def totalLoanAmount(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("select state, SUM(loan_amount) as totalLoanAmount from Partition_Data2  group by state ")
        result.show()    
}

def top25LoansPurposes(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("select emp_title As EmploymentTitle, loan_purpose As LoanPurpose, verified_income As Income_Verified from Partition_Data2  limit 25 ")
        result.show()    
}
def jobWithLoanMoreThan20K(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("select emp_title As EmploymentTitle, loan_amount from Partition_Data2 where loan_amount >20000 ")
        result.show()    
}

def stateWithHighDeliq(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("select distinct(state), loan_status, SUM(num_historical_failed_to_pay) as TotalDeliquency from Partition_Data2 group by state, loan_status")
        result.show()    
}

def numOfDebtConsolidation(hiveCtx:HiveContext): Unit = {
        val result = hiveCtx.sql("select emp_title As EmploymentTitle, loan_status as FullyPaidCount from Partition_Data2 where loan_status='Fully Paid'")
        result.show()    
}

}//end of project
