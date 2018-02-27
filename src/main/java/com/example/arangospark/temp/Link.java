package com.example.arangospark.temp;

import java.io.Serializable;

import com.arangodb.entity.DocumentField;
import com.arangodb.entity.DocumentField.Type;

public class Link implements Serializable{
	private static final long serialVersionUID = 1L;

	@DocumentField(Type.ID)
	private String _id;

	@DocumentField(Type.KEY)
	private String _key;

	@DocumentField(Type.REV)
	private String _revision;

	@DocumentField(Type.FROM)
	private String fromNodeId;

	@DocumentField(Type.TO)
	private String toNodeId;

	private String add_to_cart_order,reordered;
	
	public Link() {}
	
    public Link(String fromNodeId, String toNodeId, String label) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this._key = label;
    }
    
	public String getreordered() {
		return this.reordered;
	}
	public void setreordered(String reordered) {
		this.reordered = reordered;
	}
	public String getadd_to_cart_order() {
		return this.add_to_cart_order;
	}
	public void setadd_to_cart_order(String add_to_cart_order) {
		this.add_to_cart_order = add_to_cart_order;
	}
	public String fromNode() {
        return fromNodeId;
    }
    public String toNode() {
        return toNodeId;
    }
    public boolean hasProp() {
    	try {
    		if (this.reordered != null) {
    			return true;
    		}
    	} catch (Exception e) {
    		return false;
    	}
    	return false;
    }
	public void setFromNodeId(String newFrom) {
		this.fromNodeId = newFrom;
	}
	public void setfromNodeId(String newTo) {
		this.toNodeId = newTo;
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