package data.yoochoose;

public class Item {
  
  private int id;
  
  private int priceChangeCount = 0;
  
  private int catChangeCount = 0;
  
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

  public void setPurchased(boolean inPurchased) {
    this.purchased = inPurchased;
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
    priceChangeCount++;
  }

  public int getCategoryId() {
    return categoryId;
  }

  public void setCategoryId(int categoryId) {
    this.categoryId = categoryId;
    priceChangeCount++;
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

  public int getPriceChangeCount() {
    return priceChangeCount;
  }

  public int getCatChangeCount() {
    return catChangeCount;
  }
}
