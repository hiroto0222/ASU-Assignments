package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // check if coordinate is inbounds
  def inBounds(x: Double, y: Double, z: Double, minX: Double, minY: Double, minZ: Double, maxX: Double, maxY: Double, maxZ: Double): Boolean = {
    (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ)
  }

  // check boundaries
  def checkBoundary(point: Int, minVal: Int, maxVal: Int): Int = {
    if (point == minVal || point == maxVal) {
      return 1
    }
    0
  }

  // get neighbour count
  def getNeighbourCount(x: Int, y: Int, z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int = {
    // map to space time cube
    val pointLocationInCube: Map[Int, String] = Map(0 -> "inside", 1 -> "face", 2 -> "edge", 3 -> "corner")
    val mapping: Map[String, Int] = Map("inside" -> 26, "face" -> 17, "edge" -> 11, "corner" -> 7)

    // get initial state
    var initialState = 0;
    initialState += checkBoundary(x, minX, maxX)
    initialState += checkBoundary(y, minY, maxY)
    initialState += checkBoundary(z, minZ, maxZ)

    // get location
    val location = pointLocationInCube(initialState)
    mapping(location)
  }

  // get G score
  def getGScore(x: Int, y: Int, z: Int, numCells: Int, mean: Double, sd: Double, totalNeighbours: Int, sumOfAllNeighbourPoints: Int): Double = {
    val numerator = sumOfAllNeighbourPoints.toDouble - (mean * totalNeighbours.toDouble)
    val denominator = sd * math.sqrt(((numCells.toDouble * totalNeighbours.toDouble) - (totalNeighbours.toDouble * totalNeighbours.toDouble)) / (numCells.toDouble - 1.0))
    (numerator / denominator)
  }
}
