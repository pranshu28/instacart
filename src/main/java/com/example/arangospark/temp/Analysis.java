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

public class Analysis  implements Serializable{
	private static final long serialVersionUID = 1L;
	private ArangoJavaRDD<Node> nodes;
	private ArangoJavaRDD<Link> links;
	
	public Analysis(ArangoJavaRDD<Node> rddv, ArangoJavaRDD<Link> rdde,String chart,String path) throws IOException {
		this.nodes = rddv;
		this.links = rdde;
		if(chart.equals("1")) {
			chart1(path);
		}
		else {
			chart2(path);
		}
	}
	@SuppressWarnings("serial")
	public void chart1(String path) throws IOException {
		/*---All Aisle nodes---*/
		List<Node> aislename = nodes.filter(x -> x.getKey().contains("aisle")).collect();
		/*---All aisle Links to products---*/
		JavaRDD<Link> aisle = links.filter(temp -> temp.fromNode().contains("aisle"));
		/*---Pairs of AisleName and ProductID---*/
		List<Tuple2<String,String>> aisle_prod = aisle.filter(e -> e.getKey().contains("aisle")).mapToPair(
			new PairFunction<Link,String,String>() {
				public Tuple2<String,String> call(Link x) {
					String a=null;
					for (Node i:aislename) {
						if (i.getId().contains(x.fromNode())) {
							a = i.getaisle();
							break;
						}
					}
					return new Tuple2<String,String>(a,x.toNode());
				}
			}).collect();
		/*---Pairs of AisleName and ReorderedList---*/
		JavaPairRDD<String, Iterable<Integer>> orders = links.filter(t -> t.hasProp()).mapToPair(
			new PairFunction<Link, String, Integer>() {
				public Tuple2<String, Integer> call(Link x) {
					String a=null;
					for (Tuple2<String, String> i:aisle_prod) {
						if (i._2.contains(x.toNode())) {
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
		JavaRDD<Link> products = links.filter(temp -> (temp.fromNode().contains("order") && temp.toNode().contains("product")));
		/*---Pairs of ProductName with corresponding OrderHour---*/
		JavaPairRDD<String,Integer> product = products.mapToPair(
			new PairFunction<Link,String,Integer>() {
				public Tuple2<String,Integer> call(Link x) {
					Integer hour=null;
					for (Node i:orderhour) {
						if (i.getId().contains(x.fromNode())) {
							hour = Integer.parseInt(i.getorder_hour_of_day());
							break;
						}
					}
					String name=null;
					for (Node i:productname) {
						if (i.getId().contains(x.toNode())) {
							name = i.getproduct_name();
							break;
						}
					}
					return new Tuple2<String,Integer>(name,hour);
				}
			}).filter(x -> (x._1!=null && x._2!=null));	
		
		/*---Count distinct product ordered---*/
		JavaPairRDD<String, Long> count = product.countApproxDistinctByKey(0.05);
		/*---Sort the product based on count---*/
		JavaPairRDD<Long, String> swappedPair = count.mapToPair(
			new PairFunction<Tuple2<String, Long>, Long, String>() {
	           public Tuple2<Long, String> call(Tuple2<String, Long> item) throws Exception {
	               return item.swap();
	           }
	        }).sortByKey(false);

		/*---This file will contain top 20 products with columns: Product Name, % order in Hours---*/
		PrintWriter out = new PrintWriter(new FileWriter(path+"/chart2_data.txt"));
		out.print("product,hour_distribution");
		int j=0;
		for (Tuple2<Long, String> popular:swappedPair.collect()) {
			j++;
			if(j>10) {break;}
			out.print("\n"+popular._2+",");
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
