/**
 * @author Tao Chen
 *
 * Method computeDistance for computing distance between two coordinates
 * Method passCircle for checking whether the taxi trip passes airport circle or not
 */

package tripDist;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.TimeZone;

public class Distance {

  public static double computeDistance (double startLat, double startLong, double stopLat, double stopLong) {
    double deltaLat = stopLat - startLat;
    double deltaLong = stopLong - startLong;
    double meanLat = (startLat + stopLat) / 2.0;
    deltaLat = Math.toRadians(deltaLat);
    deltaLong = Math.toRadians(deltaLong);
    meanLat = Math.toRadians(meanLat);

    double distance, R = 6371.009;  // kilometer
    distance = R * Math.sqrt(deltaLat*deltaLat + Math.pow(Math.cos(meanLat) * deltaLong, 2));

    return distance;
  }

  public static boolean passCircle (double circleLat, double circleLong,
                                    double startLat, double startLong, double stopLat, double stopLong) {
    boolean res = false;
    double a, b, c;
    a = computeDistance(startLat, startLong, stopLat, stopLong);
    b = computeDistance(startLat, startLong, circleLat, circleLong);
    c = computeDistance(stopLat, stopLong, circleLat, circleLong);

    if (c+b == a) { res = true;}
    if (c*c >= a*a + b*b && b <= 1.0) { res = true;}
    if (b*b >= a*a + c*c && c <= 1.0) { res = true;}

    double p0 = (a+b+c) / 2.0;
    double s = Math.sqrt(p0 * (p0-a) * (p0-b) * (p0-c));
    if ((2.0*s / a) <= 1.0) { res = true;}

    return res;
  }

    public static void main (String[] args) throws IOException {
      File fn = new File("/Users/enzo/Dropbox/CT/ArtificialIntelligence/BigDataAnalyticsProgramming/Assignments/assignment4/2010_03.segments");
      BufferedReader br;
      final TimeZone timeZone = TimeZone.getTimeZone("America/San Francisco");

      try {
        br = new BufferedReader(new FileReader(fn));
        String line = null;
        int count = 0;
        while ((line = br.readLine()) != null) {
          String[] values = line.split(",");
          if (values.length < 7) {
            System.out.println(line);
            System.out.println("Some values missing in this line!\n");
            break;
          }

          //double t1 = Double.parseDouble(values[1]);
          //double t2 = Double.parseDouble(values[4]);
          double startLat, startLong, stopLat, stopLong;

          //Date d1 = new Date((long) t1*1000);
          //Date d2 = new Date((long) t2*1000);

          startLat = Double.parseDouble(values[2]);
          startLong = Double.parseDouble(values[3]);
          stopLat = Double.parseDouble(values[6]);
          stopLong = Double.parseDouble(values[7]);

          double distance;
          distance = computeDistance(startLat, startLong, stopLat, stopLong);
          boolean pass = false;
          double circleLat = 37.62131, circleLong = -122.37896;
          pass = passCircle(circleLat, circleLong, startLat, startLong, stopLat, stopLong);
          System.out.print("These two coordinates pass the airport or not: ");
          System.out.print(pass);
          System.out.println();
          count += 1;
          if (count > 100) {break;}

          /*
          // speed of airplane: 1000 kilometer per hour (3600 seconds) = 0.3
          // speed of walking: 3 kilometer per hour (3600 seconds) = 0.001
          if (distance / (t2-t1) > 0.1) {
              System.out.println();
              System.out.println(line);

              System.out.println(d1);
              System.out.println(d2);
              System.out.println(t2 - t1);

              System.out.println(distance); // kilometers
          }*/

        }

      } catch (IOException e) {
        e.printStackTrace();
      }


    }
}
