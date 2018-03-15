package com.example.arangospark.temp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import com.arangodb.ArangoDBException;

public class Csv {

    public Csv(Arango db,String mainFolder) throws IOException {
		File folder = new File(mainFolder);
		for (File fileEntry : folder.listFiles()) {
			String file = fileEntry.getName();
			if (file.contains(".csv")){
				System.out.println("Parsing: "+file);
				BufferedReader br = new BufferedReader(new FileReader(mainFolder+file));
				String[] header = br.readLine().split(",");
				loop(file,br,db,header);
			}
		}
    }
    
    /*---This loop with parse each CSV file in the Instacart data---*/
    private static void loop(String file,BufferedReader br, Arango db,String[] header) throws ArangoDBException, IOException {
    	String line;
    	while ((line = br.readLine()) != null) {
    		String[] row = line.split(",");
    		ArrayList<String> meta_key = new ArrayList<String>();
			ArrayList<String> meta_value = new ArrayList<String>();
    		/*---First columns in all CSVs will define a new Node, calling it as main_node---*/
    		db.createNode(new Node(header[0]+"_"+row[0]));
			String main_node = "vertices/"+header[0]+"_"+row[0];
			
			/*---To avoid parsing errors specifically in products.csv---*/
			if (file.equals("products.csv")) {
				String[] temp = line.split(",");
				String name = String.join(",", Arrays.copyOfRange(temp, 1, temp.length-2));
				row[0] = temp[0];
				row[1] = name;
				row[2] = temp[temp.length-2];
				row[3] = temp[temp.length-1];
				String[] froms = {"aisle_id","department_id"};
				String[] from_id = new String[2];
				for (int k=0;k<froms.length;k++) {
					db.createNode(new Node(froms[k]+"_"+row[k+2]));
					from_id[k] = "vertices/"+froms[k]+"_"+row[k+2];
				}
				db.createLink(new Link(from_id[1],from_id[0],froms[1]+"_"+row[3]+"_"+froms[0]+"_"+row[2]));
				db.createLink(new Link(from_id[0],main_node,froms[0]+"_"+row[2]+"_product_id"+"_"+row[0]));
			}
			
			/*---Define Meta-data as well as Links to other Nodes for main_node---*/
			for (int d=1;d<header.length;d++) {
				try {
					String id = header[d].substring(header[d].length()-3);
					
					/*---Header with IDs in other columns are Nodes that are Linked with the main_node---*/
					if (id.equals("_id") && !file.equals("products.csv")) {
						db.createNode(new Node(header[d]+"_"+row[d]));
						db.createLink(new Link(main_node,"vertices/"+header[d]+"_"+row[d],header[0]+"_"+row[0]+"_"+header[d]+"_"+row[d]));
					}
					
					/*---Other columns are meta-data that will act as properties of the main_node---*/
					else if (!id.equals("_id") && !db.updated(header[0]+"_"+row[0])){
						meta_key.add(header[d]);
						try {
							if (file.contains("products.csv")) {
								meta_value.add(row[d]+" ");
							}
							else {
								meta_value.add(row[d]);
							}
						} catch (Exception e) {
							meta_value.add(" "); //There were some missing values too in Orders.csv
						}
					}
				} catch(Exception e) {
					System.err.println("Error occured: "+e);
				}
			}
			
			/*---Update Link meta-data from Order_Products__*.csv files---*/
			if (file.contains("order_products__") && !meta_key.isEmpty()) {
				db.updateLink(header[0]+"_"+row[0]+"_"+header[1]+"_"+row[1],meta_key,meta_value);
			}
			/*---Update Node meta-data from other CSV files---*/
			if (!db.updated(header[0]+"_"+row[0])) {
				db.updateNode(header[0]+"_"+row[0],meta_key,meta_value);
			}
		}
    }
}
