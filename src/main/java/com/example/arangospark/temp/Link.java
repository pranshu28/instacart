package com.example.arangospark.temp;

import java.io.Serializable;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;

public class Node implements Serializable{
	private static final long serialVersionUID = 1L;

	@DocumentField(Type.ID)
	private String _id;
	
	@DocumentField(Type.KEY)
	private String _key;
	
	@DocumentField(Type.REV)
	private String _revision;
	    
	private String eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order,aisle,department,product_name;
	
	public Node(String label) {
	    this._key = label;
	}
	public Node() {}
	
	public boolean isOrder() {
		try {
			if (this.order_hour_of_day != null) {
				return true;
	    	}
	    } catch (Exception e) {
	    	return false;
	   	}
	   	return false;
	}
	public String getproduct_name() {
		return this.product_name;
	}
	public void setproduct_name(String product_name) {
		this.product_name = product_name;
	}
	public String getdepartment() {
		return this.department;
	}
	public void setdepartment(String department) {
		this.department = department;
	}
	public String getaisle() {
		return this.aisle;
	}
	public void setaisle(String aisle) {
		this.aisle = aisle;
	}
	public String getdays_since_prior_order() {
		return this.days_since_prior_order;
	}
	public void setdays_since_prior_order(String days_since_prior_order) {
		this.days_since_prior_order = days_since_prior_order;
	}
	public String getorder_hour_of_day() {
		return this.order_hour_of_day;
	}
	public void setorder_hour_of_day(String order_hour_of_day) {
		this.order_hour_of_day = order_hour_of_day;
	}
	public String getorder_dow() {
		return this.order_dow;
	}
	public void setorder_dow(String order_dow) {
		this.order_dow = order_dow;
	}
	public String geteval_set() {
		return this.eval_set;
	}
	public void setorder_number(String order_number) {
		this.order_number = order_number;
	}
	public String getorder_number() {
		return this.order_number;
	}
	public void seteval_set(String eval_set) {
		this.eval_set = eval_set;
	}
	public String getId() {
		return this._id;
	}
	public void setId(String newId) {
		this._id = newId;
	}
	public String getKey() {
		return this._key;
	}
	public void setKey(String newKey) {
		this._key = newKey;
	}
}
