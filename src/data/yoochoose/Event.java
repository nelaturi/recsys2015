package data.yoochoose;

import java.time.LocalDateTime;

/**
 * 
 * @author hsheil
 *
 */
public abstract class Event {
  
  public enum Type {CLICK, PURCHASE}

  protected LocalDateTime date;
  protected int itemId;

  public LocalDateTime getDate() {
    return date;
  }

  public void setDate(LocalDateTime date) {
    this.date = date;
  }

  public int getItemId() {
    return itemId;
  }

  public void setItemId(int itemId) {
    this.itemId = itemId;
  }

}
