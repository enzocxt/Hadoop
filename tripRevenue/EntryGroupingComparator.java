/**
 * @author Tao Chen
 *
 * Key grouping class for Entry
 */

package tripRevenue;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class EntryGroupingComparator extends WritableComparator {
  public EntryGroupingComparator() {
    super(Entry.class, true);
  }

  public int compare(WritableComparable a, WritableComparable b) {
    Entry a1 = (Entry) a;
    Entry b1 = (Entry) b;
    return (a1.getTaxiId() < b1.getTaxiId()) ? -1 : ((a1.getTaxiId() > b1.getTaxiId()) ? 1 : 0);
  }
}
