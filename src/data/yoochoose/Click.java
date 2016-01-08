package data.yoochoose;

/**
 * 
 * @author hsheil
 *
 */
public class Click extends Event {

  private int categoryId;
  private boolean isSpecial;

  public boolean isSpecial() {
    return isSpecial;
  }

  public void setSpecial(boolean isSpecial) {
    this.isSpecial = isSpecial;
  }

  public int getCategoryId() {
    return categoryId;
  }

  public void setCategoryId(int categoryId) {
    this.categoryId = categoryId;
  }

}
