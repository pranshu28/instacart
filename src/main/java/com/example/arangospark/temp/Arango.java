package com.example.arangospark.temp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.util.MapBuilder;

public class Arango {
	
	private static String GRAPH_NAME = "mygraph"; 
	protected static String EDGE_COLLECTION_NAME = "edges";
	protected static String VERTEXT_COLLECTION_NAME = "vertices";
	public static ArrayList<String> node_list = new ArrayList<String>();
	public static ArrayList<String> e = new ArrayList<String>();
	public static ArrayList<Boolean> u = new ArrayList<Boolean>();
	static ArangoDatabase database = new ArangoDB.Builder().user("root").build().db();
	
	public Arango(String user,String passwd, String graph, String edge, String vertex) {
		GRAPH_NAME = graph;
		EDGE_COLLECTION_NAME = edge;
		VERTEXT_COLLECTION_NAME = vertex;
		database = new ArangoDB.Builder().user(user).password(passwd).build().db();
		newGraph(GRAPH_NAME,EDGE_COLLECTION_NAME,VERTEXT_COLLECTION_NAME);
    }
	
	 /*----New Database----*/ 
	public ArangoDatabase newDatabase(String name,boolean create) {
		ArangoDB arangoDB = new ArangoDB.Builder().user("root").build();
		if (create) {
			try {
				arangoDB.db(name).drop();
			} catch (final ArangoDBException e) {}
			arangoDB.createDatabase(name);
		}
		return arangoDB.db(name);
	}
	
    /*----New Graph----*/
	public void newGraph(String name,String edge,String vertex){
		final Collection<EdgeDefinition> edgeDefinitions = new ArrayList<EdgeDefinition>();
		final EdgeDefinition edgeDefinition = new EdgeDefinition().collection(edge).from(vertex).to(vertex);
		edgeDefinitions.add(edgeDefinition);
		try {
			database.createGraph(name, edgeDefinitions, null);
		} catch (final Exception ex) {}
	}
	
	/*----New Collection----*/
	public void newCollection(String name) {
		try {
			database.collection(name).drop();
		} catch (final ArangoDBException e) {}
		database.createCollection(name);
	}

	/*----Create new node - to vertex collection----*/
	public void createNode(Node node) throws ArangoDBException{
		try{
			database.graph(GRAPH_NAME).vertexCollection(VERTEXT_COLLECTION_NAME).insertVertex(node);
			node_list.add(node.getKey());
			u.add(false);
		} catch(Exception e) {
			//Already Exists
		}
	}
	

	/*----Create new link - to edge collection----*/
	public void createLink (Link link) throws ArangoDBException{
		try{
			e.add(link.getKey());
			database.graph(GRAPH_NAME).edgeCollection(EDGE_COLLECTION_NAME).insertEdge(link);
		} catch(ArangoDBException e) {
			//Already Exists
		}
	}
	
	/*---Check if the node is already updated with meta-data---*/
	public boolean updated(String id) {
		try {
			return u.get(node_list.indexOf(id));
		}
		catch (Exception e) {
			return false;
		}
	}
	
	/*---Update the link with given meta-data---*/
	public void updateLink(String link,ArrayList<String> vertexkey,ArrayList<String> updates) {
		String temp="";
		for (int i=0;i<vertexkey.size();i++) {
			temp = temp +" '" + vertexkey.get(i) + "':'" + updates.get(i) + "',";
		}
		String query = "FOR u IN "+EDGE_COLLECTION_NAME+" FILTER u._key == '" + link + "' UPDATE u WITH { "
				+ temp.substring(0,temp.length()-1) + "} IN "+EDGE_COLLECTION_NAME;
		
		try {
			Map<String, Object> bindVars = new MapBuilder().get();
			database.query(query, bindVars, null,BaseDocument.class);	
		} catch (Exception e) {
			System.err.println("Error Occuered: "+e+" in query:\n"+query);
		}
	}
	
	/*---Update the node with given meta-data---*/
	public void updateNode(String node,ArrayList<String> vertexkey,ArrayList<String> updates) {
		String temp="";
		for (int i=0;i<vertexkey.size();i++) {
			temp = temp +" '" + vertexkey.get(i) + "':'" + updates.get(i).replaceAll("[\'\"\\-+.^:,]","") + "',";
		}
		String query = "FOR u IN "+VERTEXT_COLLECTION_NAME+" FILTER u._key == '" + node + "' UPDATE u WITH { "
				+ temp.substring(0,temp.length()-1) + "} IN "+VERTEXT_COLLECTION_NAME;
		try {
			Map<String, Object> bindVars = new MapBuilder().get();
			database.query(query, bindVars, null,BaseDocument.class);
			try {
				u.set(node_list.indexOf(node), true);
			} catch(Exception e) {/*u was empty*/}
		} catch (Exception e) {
			System.err.println("Error Occuered: "+e+" in query:\n"+query);
		}
	}
}
