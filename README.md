# instacart analysis

(Under Construction)*

#### File description: 

Code in `/src/main/java/com/example/arangospark/temp/`

1. `Node.java` and `Link.java`: It contains a generic definition for all vertices (eg. orders, products, aisles, departments, users) and links between them. 
2. `Csv.java`: Contains Parsing methods for all CSV files *specifically* in instacart data.
3. `Arango.java`: It has basic ArangoDB functions used for building graph for instacart data. (eg. newDatabase, createNode, createLink, updateLink, updateNode etc.)
4. `Analysis.java`: The two chart analysis is done in this file. 
*Currently working on using Spark SQL instead of Core functions.*
5. `App.java`: This contains the main function and takes arguments `arg1` and `arg2`: 
    * `0 path/to/instacart/folder`: For building graph in ArangoDB.
    * `1 path/to/save/chart1/`: For building chart 1 analysis. 
    This will create a text file that will have three columns: *AisleName*, *Number of purchased products*, *Percentage of Reordered Products*. Check `chart1_data.txt`.
    * `2 path/to/save/chart2/`: For building chart 2 analysis. 
    This file will contain top, say 10, products with columns: *Product Name*, *Space-separated list: % of product ordered in 24 Hours*. Check `chart2_data.txt`. 

#### Excecution:
(Assuming ArangoDB and Spark are already installed.) 
1. Clone/Download the repository.
2. Build the jar file with all dependencies using this command:
    `mvn clean compile assembly:single`
3. To run the `*.jar`file in Apark Environment:
    `spark-submit --class com.example.arangospark.temp.App "/path/to/this/repo/target/*.jar" arg1 arg2`
    
#### Note:
* The graph was built on a subset of data, hence the analysis is not exactly same.
* Till now, analysis has been done using simple spark functions and will be optimized in future. (Using Spark MLlib/GraphX/SQL)