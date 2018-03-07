package com.example.arangospark.temp;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import com.arangodb.spark.rdd.api.java.ArangoJavaRDD;

import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Analysis  implements Serializable{
	private static final long serialVersionUID = 1L;
	private ArangoJavaRDD<Node> nodes;
	private ArangoJavaRDD<Link> links;
	
	public Analysis(ArangoJavaRDD<Node> rddv, ArangoJavaRDD<Link> rdde,String chart,String path) throws IOException {
		this.nodes = rddv;
		this.links = rdde;
		SparkSession spark = SparkSession.builder().getOrCreate();
		Dataset<Row> nodeDF = spark.createDataFrame(this.nodes, Node.class);
		Dataset<Row> linkDF = spark.createDataFrame(this.links, Link.class);
		nodeDF.createOrReplaceTempView("node");
		linkDF.createOrReplaceTempView("link");
		if(chart.equals("1")) {
//			chart_sql1(path,spark);
			chart1(path);
		}
		else {
//			chart_sql2(path,spark);
			chart2(path);
		}
	}
	public void chart_sql1(String path,SparkSession spark) {
		Dataset<Row> aisles = spark.sql("select from,to from link where key like concat('aisle', '%')");
		aisles.createOrReplaceTempView("aisle_");
		Dataset<Row> orders = spark.sql("select from,to,reordered from link where key like concat('order','%','product','%') and reordered != 'NULL'");
		orders.createOrReplaceTempView("order_");
		Dataset<Row> data = spark.sql("select node.aisle,cast(order_.reordered AS integer) from aisle_ inner join order_ on (aisle_.to = order_.to) inner join node on (aisle_.from = node.id)");
		data.createOrReplaceTempView("data1");
		Dataset<Row> analysis = spark.sql("select aisle,count(reordered) as purchased,avg(reordered) as percent_reordered from data1 group by aisle");
		analysis.show();
	}
	public void chart_sql2(String path,SparkSession spark) {
		Dataset<Row> links = spark.sql("select from,to from link where key like concat('order','%','product','%')");
		links.createOrReplaceTempView("ordpro");
		Dataset<Row> pairs = spark.sql("select n2.product_name,cast(n1.order_hour_of_day AS integer) from ordpro inner join node n1 on (ordpro.from=n1.id) inner join node n2 on (ordpro.to=n2.id) where n2.product_name!='NULL' and n1.order_hour_of_day!='NULL'");
		pairs.createOrReplaceTempView("data2");
		Dataset<Row> early = spark.sql("select product_name,count(order_hour_of_day) as cnt from data2 where order_hour_of_day<=12 group by product_name order by cnt desc limit 10");
		early.createOrReplaceTempView("early");
		Dataset<Row> late = spark.sql("select product_name,count(order_hour_of_day) as cnt from data2 where order_hour_of_day>12 group by product_name order by cnt desc limit 10");
		late.createOrReplaceTempView("late");
		Dataset<Row> a_early = spark.sql("select data2.product_name,sort_array(collect_list(data2.order_hour_of_day)) as hours,count(data2.order_hour_of_day) as cntall from data2 inner join early on data2.product_name=early.product_name group by data2.product_name order by cntall");
		a_early.createOrReplaceTempView("early");
		a_early.show();
	}
	
	
	@SuppressWarnings("serial")
	public void chart1(String path) throws IOException {
		/*---All Aisle nodes---*/
		List<Node> aislename = nodes.filter(x -> x.getKey().contains("aisle")).collect();
		/*---All aisle Links to products---*/
		JavaRDD<Link> aisle = links.filter(temp -> temp.getFrom().contains("aisle"));
		/*---Pairs of AisleName and ProductID---*/
		List<Tuple2<String,String>> aisle_prod = aisle.filter(e -> e.getKey().contains("aisle")).mapToPair(
			new PairFunction<Link,String,String>() {
				public Tuple2<String,String> call(Link x) {
					String a=null;
					for (Node i:aislename) {
						if (i.getId().contains(x.getFrom())) {
							a = i.getaisle();
							break;
						}
					}
					return new Tuple2<String,String>(a,x.getTo());
				}
			}).collect();
		/*---Pairs of AisleName and ReorderedList---*/
		JavaPairRDD<String, Iterable<Integer>> orders = links.filter(t -> t.hasProp()).mapToPair(
			new PairFunction<Link, String, Integer>() {
				public Tuple2<String, Integer> call(Link x) {
					String a=null;
					for (Tuple2<String, String> i:aisle_prod) {
						if (i._2.contains(x.getTo())) {
							a = i._1;
							break;
						}
					}
					return new Tuple2<String, Integer>(a, Integer.parseInt(x.getreordered()));
				}
			}).groupByKey();
		
		/*---This file will contain three columns: AisleName, Number of purchased products, Percentage of Reordered Products---*/
		PrintWriter out = new PrintWriter(new FileWriter(path+"/chart1_data.txt"));
		out.println("aisle,product_purchased,reordered");
		for (Tuple2<String, Iterable<Integer>> i:orders.collect()) {
			int total=0;
			float re=0;
			Iterator<Integer> arr = i._2.iterator();
			while(arr.hasNext()) {
				total++;re+=arr.next();
			}
			re = 100*re/total;
			out.println(i._1+","+total+","+re);
		}
		out.close();
	}
	
	public void chart2(String path) throws IOException {
		/*---All Product nodes---*/
		List<Node> productname = nodes.filter(x -> x.getKey().contains("product")).collect();
		/*---All Order nodes---*/
		List<Node> orderhour = nodes.filter(x -> x.isOrder()).collect();
		/*---All Order-Product Links---*/
		JavaRDD<Link> products = links.filter(temp -> (temp.getFrom().contains("order") && temp.getTo().contains("product")));
		/*---Pairs of ProductName with corresponding OrderHour---*/
		JavaPairRDD<String,Integer> product = products.mapToPair(
			new PairFunction<Link,String,Integer>() {
				public Tuple2<String,Integer> call(Link x) {
					Integer hour=null;
					for (Node i:orderhour) {
						if (i.getId().contains(x.getFrom())) {
							hour = Integer.parseInt(i.getorder_hour_of_day());
							break;
						}
					}
					String name=null;
					for (Node i:productname) {
						if (i.getId().contains(x.getTo())) {
							name = i.getproduct_name();
							break;
						}
					}
					return new Tuple2<String,Integer>(name,hour);
				}
			}).filter(x -> (x._1!=null && x._2!=null));	
		
		/*---Count distinct product ordered---*/
		JavaPairRDD<String, Long> prod_early = product.filter(i -> i._2<=12).countApproxDistinctByKey(0.05);
		JavaPairRDD<String, Long> prod_late = product.filter(i -> i._2>12).countApproxDistinctByKey(0.05);
		
		/*---Sort the product based on count---*/
		PairFunction swap = new PairFunction<Tuple2<String, Long>, Long, String>() {
	           public Tuple2<Long, String> call(Tuple2<String, Long> item) throws Exception {
	               return item.swap();
	           }
	        };
		JavaPairRDD<Long, String> swapped_early = prod_early.mapToPair(swap).sortByKey(false);
		JavaPairRDD<Long, String> swapped_late = prod_late.mapToPair(swap).sortByKey(false);
		

		/*---This file will contain top 20 products with columns: Product Name, % order in Hours---*/
		PrintWriter out = new PrintWriter(new FileWriter(path+"/chart2_data.txt"));
		out.print("product,class,hour_distribution");
		int j=0;
		for (Tuple2<Long, String> popular:swapped_early.collect()) {
			j++;
			if(j>10) {break;}
			out.print("\n"+popular._2+",early,");
			int total=0;
			double[] hours = new double[24];
			Arrays.fill(hours, 0.0);
			for (Tuple2<String, Integer> pop:product.filter(e -> e._1.contains(popular._2)).collect()) {
				if (pop._2>0 && pop._2<25) {
					total+=1;
					hours[pop._2-1]+=1;
				}
			}
			for (int hour=0;hour<24;hour++) {
				hours[hour] = 100*hours[hour]/total;
				out.print(" "+hours[hour]);
			}
		}
		j=0;
		for (Tuple2<Long, String> popular:swapped_late.collect()) {
			j++;
			if(j>10) {break;}
			out.print("\n"+popular._2+",late,");
			int total=0;
			double[] hours = new double[24];
			Arrays.fill(hours, 0.0);
			for (Tuple2<String, Integer> pop:product.filter(e -> e._1.contains(popular._2)).collect()) {
				if (pop._2>0 && pop._2<25) {
					total+=1;
					hours[pop._2-1]+=1;
				}
			}
			for (int hour=0;hour<24;hour++) {
				hours[hour] = 100*hours[hour]/total;
				out.print(" "+hours[hour]);
			}
		}
		out.close();
	}	
}
