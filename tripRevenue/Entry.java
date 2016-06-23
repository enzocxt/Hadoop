package tripRevenue;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Entry implements WritableComparable<Entry> {
    private int taxiId;
    private String startTime;

    public Entry() {
    }

    public int compareTo(Entry entry) {
        //int result = this.taxiId.compareTo(entry.getTaxiId());
        int result = (this.taxiId < entry.getTaxiId()) ? -1 : (this.taxiId > entry.getTaxiId() ? 1 : 0);
        if (result == 0) {
            result = compare(startTime, entry.getStartTime());
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(taxiId);
        dataOutput.writeUTF(startTime);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.taxiId = dataInput.readInt();
        this.startTime = dataInput.readUTF();
    }

    public int getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(int taxiId) {
        this.taxiId = taxiId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public static int compare(String a, String b) {
        /*
        TimeZone.setDefault(TimeZone.getTimeZone("America/San Francisco"));
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

        Date t1 = new Date(), t2 = new Date();
        try {
            t1 = format.parse(a);
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
        try {
            t2 = format.parse(b);
        } catch (ParseException pe) {
            pe.printStackTrace();
        }*/
        //return a < b ? -1 : (a > b ? 1 : 0);
        //return t1.before(t2) ? -1 : (t2.before(t1) ? 1 : 0);
        return a.compareTo(b);
    }

    public String toString() {
        return Integer.toString(taxiId);
    }
}