/**
 * @author Tao Chen
 *
 * TaxiTrips class for one taxi.
 * The trips attribute is a list of Trip object belonging to this taxi.
 */

package tripRevenue;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class TaxiTrips implements WritableComparable<TaxiTrips> {
  private int taxiId;
  private ArrayList<Trip> trips = new ArrayList<Trip>();

  public TaxiTrips() {
  }

  public int compareTo(TaxiTrips taxiTrips) {
      return (taxiId < taxiTrips.taxiId) ? -1 : ((taxiId > taxiTrips.taxiId) ? 1 : 0);
  }

  public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(taxiId);

      StringBuilder result = new StringBuilder();
      int i = 0;
      while (i < trips.size()-1) {
          result.append(trips.get(i).toString());
          result.append("\n");
          i += 1;
      }
      result.append(trips.get(i));

      dataOutput.writeUTF(result.toString());
  }

  public void readFields(DataInput dataInput) throws IOException {
      this.taxiId = dataInput.readInt();
      String result = dataInput.readUTF();
      String[] values = result.split("\n");
      for (String value : values) {
          Trip trip = new Trip();
          trip.setTrip(value);
          this.trips.add(trip);
      }
  }

  public int getTaxiId() {return taxiId;}

  public void setTaxiId(int taxiId) {
    this.taxiId = taxiId;
  }

  public ArrayList<Trip> getTrips() {
    return trips;
  }

  public void setTrips(ArrayList<Trip> trips) {
    this.trips = trips;
  }

  public void addTrip(Trip trip) {
    this.trips.add(trip);
  }

  public String toString() {
      StringBuilder result = new StringBuilder();

    if (trips.size() != 0) {
      int i = 0;
      while (i < trips.size() - 1) {
        result.append(Integer.toString(taxiId));
        result.append(":");
        result.append(trips.get(i).toString());
        result.append("\n");
        i += 1;
      }
      result.append(Integer.toString(taxiId));
      result.append(":");
      result.append(trips.get(i));
    }

    return result.toString();
  }
}
