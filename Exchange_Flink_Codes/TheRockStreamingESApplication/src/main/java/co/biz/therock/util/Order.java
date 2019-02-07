package co.biz.therock.util;

import java.io.Serializable;

/**
 * 
 * @author Dhinesh Raja
 *
 */
public class Order implements Serializable {

	private static final long serialVersionUID = 1L;
	private String exchangeName;
	private String time;
	private String currencyPair;
	private String orderSide;
	private Double price;
	private Double quantity;

	@Override
	public String toString() {

		return "ExchangeName: " + exchangeName + " Price: " + price + " Quantity: " + quantity + " CurrencyPair: "
				+ currencyPair + " OrderSide: " + orderSide + " TimeStamp: " + time;

	}

	public int hashCode() {
		return 1;
	}

	public boolean equals(Object obj) {
		boolean flag = false;
		Order orderObj = (Order) obj;
		if (orderObj.price == price)
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

}
