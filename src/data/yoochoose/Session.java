package data.yoochoose;

import java.util.ArrayList;
import java.util.List;

public class Session {
  
  private List<Event> events;
  
  //private int id;
  
  private boolean purchaser;

  //public Session(int inId) {
    public Session() {
    events = new ArrayList<>();
    //id = inId;
    purchaser = false;
  }

  public boolean isPurchaser() {
    return purchaser;
  }

  public void setPurchaser(boolean purchaser) {
    this.purchaser = purchaser;
  }

  public boolean add(Event e) {
    return events.add(e);
  }

  public void add(int index, Event element) {
    events.add(index, element);
  }

  public List<Event> getEvents() {
    return events;
  }
}
