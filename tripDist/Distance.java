/**
 * @author Tao Chen
 *
 * Method computeDistance for computing distance between two coordinates
 */

package tripDist;

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
}
