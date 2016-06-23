package tripRevenue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class EntryPartitioner extends Partitioner<Entry, Text> {

    public int getPartition(Entry entry, Text text, int numberPartitions) {
        return Math.abs((Integer.toString(entry.getTaxiId()).hashCode() % numberPartitions));
    }
}