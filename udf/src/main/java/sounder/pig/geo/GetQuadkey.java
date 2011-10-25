package sounder.pig.geo;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
   See: http://msdn.microsoft.com/en-us/library/bb259689.aspx

   A Pig UDF to compute the quadkey string for a given
   (longitude, latitude, resolution) tuple.
   
 */
public class GetQuadkey extends EvalFunc<String> {
    private static final int TILE_SIZE = 256;

    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 3 || input.isNull(0) || input.isNull(1) || input.isNull(2))
            return null;

        Double longitude = (Double)input.get(0);
        Double latitude = (Double)input.get(1);
        Integer resolution = (Integer)input.get(2);

        String quadKey = quadKey(longitude, latitude, resolution);
        return quadKey;        
    }

    private static String quadKey(double longitude, double latitude, int resolution) {
        int[] pixels = pointToPixels(longitude, latitude, resolution);
        int[] tiles = pixelsToTiles(pixels[0], pixels[1]);
        return tilesToQuadKey(tiles[0], tiles[1], resolution);
    }

    /**
       Return the pixel X and Y coordinates for the given lat, lng, and resolution.
     */
    private static int[] pointToPixels(double longitude, double latitude, int resolution) {
        double x = (longitude + 180) / 360;
        double sinLatitude = Math.sin(latitude * Math.PI / 180);
        double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        int mapSize = mapSize(resolution);
        int[] pixels = {(int) trim(x * mapSize + 0.5, 0, mapSize - 1), (int) trim(y * mapSize + 0.5, 0, mapSize - 1)};
        return pixels;
    }

    /**
       Convert from pixel coordinates to tile coordinates.
     */
    private static int[] pixelsToTiles(int pixelX, int pixelY) {
        int[] tiles = {pixelX / TILE_SIZE, pixelY / TILE_SIZE};
        return tiles;
    }
    
    /**
       Finally, given tile coordinates and a resolution, returns the appropriate quadkey
     */
    private static String tilesToQuadKey(int tileX, int tileY, int resolution) {
        StringBuilder quadKey = new StringBuilder();
        for (int i = resolution; i > 0; i--) {
            char digit = '0';
            int mask = 1 << (i - 1);
            if ((tileX & mask) != 0) {
                digit++;
            }
            if ((tileY & mask) != 0) {
                digit++;
                digit++;
            }
            quadKey.append(digit);
        }
        return quadKey.toString();
    }
    
    /**
       Ensure input value is within minval and maxval
     */
    private static double trim(double n, double minVal, double maxVal) {
        return Math.min(Math.max(n, minVal), maxVal);
    }

    /**
       Width of the map, in pixels, at the given resolution
     */
    public static int mapSize(int resolution) {
        return TILE_SIZE << resolution;
    }
}
