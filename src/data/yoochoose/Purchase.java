package data.yoochoose;

/**
 * 
 * @author hsheil
 *
 */
public class Purchase extends Event {

  private int price;
  private int quantity;

  public int getPrice() {
    return price;
  }

  public void setPrice(int price) {
    this.price = price;
  }

  public int getQuantity() {
    return quantity;
  }

  public void setQuantity(int quantity) {
    this.quantity = quantity;
  }
}
