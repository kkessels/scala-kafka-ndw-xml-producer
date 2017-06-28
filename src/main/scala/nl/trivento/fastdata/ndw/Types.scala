package nl.trivento.fastdata.ndw

import nl.trivento.fastdata.ndw.processor.{Heat, LatLong}
import generated.{DirectionEnum, DurationValue, GroupOfLocations, LaneEnum, Linear, LinearElementByPoints,
  MeasuredOrDerivedDataTypeEnum, MeasurementSiteRecord, Point, SiteMeasurements, TrafficFlowType, TrafficSpeed,
  TrafficStatus, TravelTimeData, VehicleCharacteristics}

case class NdwSensorId(id: String, index: Int)

trait Message {
  val id: NdwSensorId
}

object Sensor {
  def fromSiteMeasurement(site: MeasurementSiteRecord): Seq[Sensor] = {
    val id = site.id
    val locations: Option[List[LatLong]] = site.measurementSiteLocation match {
      case l: Linear => for {
        pt <- l.linearWithinLinearElement.map(_.linearElement).collect { case pt: LinearElementByPoints => pt }
        start <- pt.startPointOfLinearElement.pointCoordinates
        end <- pt.endPointOfLinearElement.pointCoordinates
      } yield List(LatLong(start), LatLong(end))
      case p: Point =>
        p.locationForDisplay.map(point => List(LatLong(point)))
      case _ =>
        None
      }
    val numberOfLanes = site.measurementSiteNumberOfLanes
    val time: Long = site.measurementSiteRecordVersionTime.map(c => c.toGregorianCalendar.getTimeInMillis).getOrElse(0)
    val direction = site.measurementSide

    for {
      e <- site.measurementSpecificCharacteristics
      location <- locations
    } yield {
      val measurementType = e.measurementSpecificCharacteristics.specificMeasurementValueType
      val vehicleCharacteristics = e.measurementSpecificCharacteristics.specificVehicleCharacteristics
      val lane = e.measurementSpecificCharacteristics.specificLane

      Sensor(NdwSensorId(id, e.index), time, direction, location, measurementType, vehicleCharacteristics,
        numberOfLanes, lane)
    }
  }
}

case class Sensor(id: NdwSensorId, time: Long, direction: Option[DirectionEnum], location: List[LatLong],
                  measurementType: MeasuredOrDerivedDataTypeEnum, vehicle: Option[VehicleCharacteristics],
                  numberOfLanes: Option[BigInt], specificLane: Option[LaneEnum]) extends Message

object Measurement {
  def fromSiteMeasurements(measurements: SiteMeasurements): Seq[Measurement] = {
    val id = measurements.measurementSiteReference.id
    measurements
      .measuredValue
      .flatMap(value => {
        val time = measurements.measurementTimeDefault.toGregorianCalendar.getTimeInMillis
        val sensorId = NdwSensorId(id, value.index)

        val data = value.measuredValue.basicData

        data match {
          case Some(speed: TrafficSpeed) => speed.averageVehicleSpeed.map(m =>
            Measurement(sensorId, time, m.speed.toDouble))
          case Some(flow: TrafficFlowType) => flow.vehicleFlow.filter(_.dataError.getOrElse(true)).map(m =>
            Measurement(sensorId, time, m.vehicleFlowRate.toDouble))
          case Some(status: TrafficStatus) => None
          case _ => None
        }
      })
  }
}

case class Measurement(id: NdwSensorId,
                       time: Long,
                       value: Double
                      ) extends Message

object MeasurementExtra {
  def fromSiteMeasurements(measurements: SiteMeasurements): Seq[MeasurementExtra] = {
    val id = measurements.measurementSiteReference.id
    measurements
      .measuredValue
      .map(value => {
        val time = measurements.measurementTimeDefault.toGregorianCalendar.getTimeInMillis
        val sensorId = NdwSensorId(id, value.index)

        val data = value.measuredValue.basicData

        val speed = data match {
          case Some(speed: TrafficSpeed) => speed.averageVehicleSpeed
            .filter(_.dataError.getOrElse(false))
            .map(_.speed)
          case _ => None
        }

        val intensity = data match {
          case Some(intensity: TrafficFlowType) => intensity.vehicleFlow
            .filterNot(_.dataError.getOrElse(false))
            .map(_.vehicleFlowRate)
          case _ => None
        }

        val travelTimes = for {
          ttd: TravelTimeData <- data.collect({ case t: TravelTimeData => t })
          fftt: DurationValue <- ttd.freeFlowTravelTime
          nett: DurationValue <- ttd.normallyExpectedTravelTime
          tt: DurationValue <- ttd.travelTime
        } yield (fftt.duration, nett.duration, tt.duration)

        MeasurementExtra(sensorId, time, speed, intensity, travelTimes)
      })
  }
}

case class MeasurementExtra(id: NdwSensorId,
                       time: Long,
                       speed: Option[Float],
                       intensity: Option[BigInt],
                       travelTimes: Option[(Float, Float, Float)]
                      ) extends Message

