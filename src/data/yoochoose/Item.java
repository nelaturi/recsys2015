package data.yoochoose;

public class Item {
  
  private int id;
  
  private int price;
  
  private int categoryId;
  
  private boolean seenWithPurchased;
  
  private boolean isMultiPurchase;
  
  private boolean purchased;
  

  public Item() {
    seenWithPurchased = false;
    isMultiPurchase = false;
    purchased = false;
  }

  public boolean isPurchased() {
    return purchased;
  }

  public void setPurchased(boolean purchased) {
    this.purchased = purchased;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getPrice() {
    return price;
  }

  public void setPrice(int price) {
    this.price = price;
  }

  public int getCategoryId() {
    return categoryId;
  }

  public void setCategoryId(int categoryId) {
    this.categoryId = categoryId;
  }

  public boolean isSeenWithPurchased() {
    return seenWithPurchased;
  }

  public void setSeenWithPurchased(boolean seenWithPurchased) {
    this.seenWithPurchased = seenWithPurchased;
  }

  public boolean isMultiPurchase() {
    return isMultiPurchase;
  }

  public void setMultiPurchase(boolean isMultiPurchase) {
    this.isMultiPurchase = isMultiPurchase;
  }
  
  
  
  

}
