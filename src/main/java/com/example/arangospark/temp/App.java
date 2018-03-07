package com.example.arangospark.temp;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.arangodb.spark.ArangoSpark;
import com.arangodb.spark.rdd.api.java.ArangoJavaRDD;

public class App {
	public static String GRAPH_NAME = "mygraph"; 
	public static String EDGE_COLLECTION_NAME = "edges";
	public static String VERTEXT_COLLECTION_NAME = "vertices";
	public static String USER = "root";
	public static String PASSWD = "";
	
	/*---Build ArangoDB Graph for Instacart---*/
	public static void buildGraph(String user,String pass,String folder) throws IOException {
		Arango db = new Arango(user,pass,GRAPH_NAME,EDGE_COLLECTION_NAME,VERTEXT_COLLECTION_NAME);
		new Csv(db,folder);
	}
	
	public static void main(String[] args) throws IOException {
		String chart="2",check_build="",s="";
//		check_build = args[0];
//		if (check_build.equals("0")) {
//			try {
//				s = args[1];
//				buildGraph(USER,PASSWD,s);
//			} catch (Exception e) {
//				System.err.println("Build error");
//			}
//		}
//		else {
//			chart = args[0];
//			s = args[1];
			SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("arango")	
	                .set("arangodb.host", "127.0.0.1")
	                .set("arangodb.port", "8529")
	                .set("arangodb.user", USER)
	                .set("arangodb.password", PASSWD);
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        ArangoJavaRDD<Node> rddv = ArangoSpark.load(sc, VERTEXT_COLLECTION_NAME, Node.class);
	        ArangoJavaRDD<Link> rdde = ArangoSpark.load(sc, EDGE_COLLECTION_NAME, Link.class);
	        new Analysis(rddv,rdde,chart,s);
	        
//		}
    }
}