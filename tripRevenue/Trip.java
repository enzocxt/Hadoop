package tripRevenue;

import java.util.ArrayList;

public class Trip {
  private String startTime = "";
  private String stopTime = "";
  // the element (lat,long): '37.7748,-122.42444'
  private ArrayList<String> trip = new ArrayList<String>();

  public Trip() {
  }

  public String getStartTime() { return startTime;}

  public void setStartTime(String startTime) { this.startTime = startTime;}

  public String getStopTime() { return stopTime;}

  public void setStopTime(String stopTime) { this.stopTime = stopTime;}

  public ArrayList<String> getTrip() {
      return trip;
  }

  public void setTrip(ArrayList<String> trip) {
      this.trip = trip;
  }

  public void setTrip(String trip) {
    String[] values = trip.split(" ");
    //ArrayList<String> result = new ArrayList<String>();
    for (String value: values) {
        this.trip.add(value);
    }
    //this.trip = result;
  }

  public void addCoor(String latitude, String longtitude) {
    StringBuilder result = new StringBuilder();
    result.append(latitude);
    result.append(",");
    result.append(longtitude);
    this.trip.add(result.toString());
  }

  public String toString() {
    StringBuilder sTrip =  new StringBuilder();

    if (trip.size() != 0 && startTime != "") {
      sTrip.append(startTime.substring(0, 10));
      sTrip.append(":");
      int i = 0;
      while (i < trip.size() - 1) {
        sTrip.append(trip.get(i));
        sTrip.append(" ");
        i += 1;
      }
      sTrip.append(trip.get(i));
    }

    return sTrip.toString();
  }

}
