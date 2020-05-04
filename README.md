

This is a publisher-subscriber pattern implementation using Java, Spring Boot and Kafka.
The application reads .csv/.xml files and transforms them into .json

**How to execute:**
1. Check out the code
2. Put .csv/.xml file in this directory:

    src/main/resources
    
3. Run zookeeper and kafka locally

4. Execute _App.java_ with following parameters:

-Dspring.profiles.active="xml,csv" file=[your input file name] 

* _-Dspring.profiles.active_ can be eather "xml" or "csv" or "xml,csv"

**How to execute xml client**

Run with following params: 
-Dspring.profiles.active="xml" input=[your input file name] 

**How to execute csv client**

Run with following params: 
-Dspring.profiles.active="csv" input=[your input file name] 

**How to execute json client**

1. Run the application with following params: 
input=[your input file name] 
2. Execute the script _resources/post_json.sh_