package org.bigdata.hpsk.HadoopTest.MR.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomerWritable implements WritableComparable<CustomerWritable>{
	
	private String orderId ;
	private int price;
	
	public CustomerWritable(){
		
	}
	public CustomerWritable(String orderId,int price){
		this.set(orderId, price);
	}
	
	public void set (String orderId,int price){
		this.setOrderId(orderId);
		this.setPrice(price);
	}
	
	public String getOrderId() {
		return orderId;
	}


	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}


	public int getPrice() {
		return price;
	}


	public void setPrice(int price) {
		this.price = price;
	}


	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.getOrderId());
		out.writeInt(this.getPrice());
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.setOrderId(in.readUTF());
		this.setPrice(in.readInt());
	}

	public int compareTo(CustomerWritable o) {
		// TODO Auto-generated method stub
		int comp = this.getOrderId().compareTo(o.getOrderId());
		if(0 == comp){
			return Integer.valueOf(this.getPrice()).compareTo(Integer.valueOf(o.getPrice()));
		}
		return comp;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((orderId == null) ? 0 : orderId.hashCode());
		result = prime * result + price;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomerWritable other = (CustomerWritable) obj;
		if (orderId == null) {
			if (other.orderId != null)
				return false;
		} else if (!orderId.equals(other.orderId))
			return false;
		if (price != other.price)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return orderId + "\t" + price;
	}
	
	

}
