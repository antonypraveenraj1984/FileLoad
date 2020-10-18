Solution :

The jar can be created as auth.jar by giving parameters in the pom.xml

The FileLoad class reads the csv file into a dataframe, the inferSchema is used to identify the datatypes for the columns.

Filter is applied to the aua column to exclude aua=650000 and stored in another dataframe 
and then with the use of withColumn sa=SERVICEMON is replaced with the value of aua column.

Finally the matching columns of aua & asa are filtered out which results in fetching the res_state_name is stored in df3
and then it is appended to the json file.

Execution :

The execute.sh file contains the spark-submit command which will execute the jar and load the final data.


Enhancements :

Enhancements like exception handling can be applied to check whether the file exists or 
the data flow completes properly can be handled.