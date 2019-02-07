package co.biz.coinbase.util;

import java.io.Serializable;

import org.json.JSONObject;
/**
 * 
 * @author Dhinesh Raja
 *
 */
public class OrderData implements Serializable {

	private static final long serialVersionUID = 1L;
	private String updateType;
	private String exchangeName;
	private String time;
	private String currencyPair;
	private String orderSide;
	private String orderId;
	private Double price;
	private Double quantity;
	private Long randomNo;


	@Override
	public String toString() {

		JSONObject order = new JSONObject();
		order.put("UpdateType", updateType);
		order.put("ExchangeName", exchangeName);
		order.put("CurrencyPair", currencyPair);
		order.put("MachineTime", time);
		order.put("OrderSide", orderSide);
		order.put("OrderId", orderId);
		order.put("Price", price);
		order.put("Quantity", quantity);
		return order.toString();
		
	}
	public int hashCode()
	{
		return 1;
	}
	
	public boolean equals( Object obj )
	{
		boolean flag = false;
		OrderData orderObj = ( OrderData )obj;
		if( orderObj.getOrderId().equals(orderId))
			flag = true;
		return flag;
	}
	public String getExchangeName() {
		return exchangeName;
	}

	public void setExchangeName(String exchangeName) {
		this.exchangeName = exchangeName;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getCurrencyPair() {
		return currencyPair;
	}

	public void setCurrencyPair(String currencyPair) {
		this.currencyPair = currencyPair;
	}

	public String getOrderSide() {
		return orderSide;
	}

	public void setOrderSide(String orderSide) {
		this.orderSide = orderSide;
	}

	

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Double getQuantity() {
		return quantity;
	}

	public void setQuantity(Double quantity) {
		this.quantity = quantity;
	}
	public String getUpdateType() {
		return updateType;
	}
	public void setUpdateType(String updateType) {
		this.updateType = updateType;
	}
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public Long getRandomNo() {
		return randomNo;
	}
	public void setRandomNo(Long randomNo) {
		this.randomNo = randomNo;
	}

}
