package gallia
package avro
package data

import aptus._
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

import org.apache.avro
import org.apache.avro.generic.GenericData

import gallia.avro.AvroImplicits._

// ===========================================================================
object AvroToGalliaData {
  import AvroLogicalType.Data.FromAvro._

  // ---------------------------------------------------------------------------
  // TODO: t220309100152 - cache decimal precisions (only reason we carry the avro schema)
  def convertRecursively(schema: Cls, avroSchema: AvroSchema)(rec: AvroRecord): Obj =
      schema
        .fields
        .flatMap { field =>
          val skey = field.skey

          Option(rec.get(skey) /* can return null */)
            .map { value =>
              skey -> processField(field)(avroSchema.getField(skey))(value) } }
        .pipe(gallia.obj)

  // ===========================================================================
  private def processField(field: Fld)(avroField: AvroField)(value: AnyValue): AnyValue =
    field.valueExtractionWithFailures {

      // ---------------------------------------------------------------------------
      nestedGalliaClass => multiple =>
          if (!multiple)      value .pipe(_convertRecursively(nestedGalliaClass, avroField.forceRecord))
          else           list(value).map (_convertRecursively(nestedGalliaClass, avroField.forceRecord)) } {

      // ---------------------------------------------------------------------------
      bsc => multiple =>
          if (!multiple)      value .pipe(basicValue(bsc, avroField))
          else           list(value).map (basicValue(bsc, avroField)) }

  // ===========================================================================  
  private def basicValue(basicType: BasicType, avroField: AvroField)(value: AnyValue): AnyValue =
    basicType match {
      case BasicType._String  => parseString(value)
      case BasicType._Boolean => value
      case BasicType._Int     => value
      case BasicType._Double  => value

      // ---------------------------------------------------------------------------
      case _: BasicType._Enm  => parseEnum(value).pipe(BasicType._Enm.parseString)

      // ---------------------------------------------------------------------------
      case BasicType._Long    => value
      case BasicType._Float   => value

      case BasicType._Byte    => toByte (forceBytes(value)) // t220311131818 - short/byte: should encode it using zigzag too
      case BasicType._Short   => toShort(forceBytes(value)) // t220311131818 - short/byte: should encode it using zigzag too

      // ---------------------------------------------------------------------------      
      case BasicType._BigInt  => toBigInt(forceBytes(value))
      case BasicType._BigDec  => toBigDec(forceBytes(value), avroField.forceDecimal.getScale) // TODO: assert precision?
      
      // ---------------------------------------------------------------------------
      case BasicType._LocalDate => value match { case daysFromEpoch: Int => toLocalDate(daysFromEpoch) }

      case BasicType._LocalTime => value match {
        case daysFromEpoch     : Int  => toLocalTimeViaMillis(daysFromEpoch)
        case microsFromMidnight: Long => toLocalTimeViaMicros(microsFromMidnight) }

      case BasicType._LocalDateTime => value match {
        case millisFromEpoch: Long if (millisFromEpoch < 10e13) => toLocalDateTimeViaMillis(millisFromEpoch)
        case microsFromEpoch: Long                              => toLocalDateTimeViaMicros(microsFromEpoch) }

      case BasicType._Instant => value match {
        case millisFromEpoch: Long if (millisFromEpoch < 10e13) => toInstantViaMillis(millisFromEpoch)
        case microsFromEpoch: Long                              => toInstantViaMicros(microsFromEpoch) }

      case BasicType._OffsetDateTime => unexpected(basicType) // not supported by avro?
      case BasicType. _ZonedDateTime => unexpected(basicType) // not supported by avro?

      // ---------------------------------------------------------------------------
      case BasicType._Binary  => ByteBuffer.wrap(forceBytes(value))
  }  

  // ===========================================================================    
  private def parseString(value: Any): String =
    value match {
      case utf8: avro.util.Utf8 => utf8.toString /* seems stable as of 1.11.0 */
      case s : String           => s /* can happen? */
      case x                    => unexpected(x) }

  // ---------------------------------------------------------------------------
  private def parseEnum(value: Any): String =      
    value match {
      case e: GenericData.EnumSymbol => e.toString /* returns the "symbol" String as of 1.11.0 */
      case s: String                 => s /* can happen? */ }

  // ---------------------------------------------------------------------------
  private def forceBytes(value: Any): Array[Byte] = bytes.apply(value)

    // ---------------------------------------------------------------------------
    private def bytes: PartialFunction[Any, Array[Byte]] = {
      case gd: GenericData.Fixed => gd.bytes
      case bb: ByteBuffer        => bb.array }

  // ===========================================================================
  private def toByte(bytes: Array[Byte]): Byte =  // t220311131818 - short/byte: should encode it using zigzag too 
    bytes
      .assert(_.size == 1)
      .head

  // ---------------------------------------------------------------------------
  private def toShort(bytes: Array[Byte]): Short = // t220311131818 - short/byte: should encode it using zigzag too 
    bytes
      .assert(_.size == 2)
      .take(2)
      .pipe(ByteBuffer.wrap( /* big-endian */(_)))
      .getShort

  // ===========================================================================
  @inline private def list(value: AnyValue): List[Any] =
    value.asInstanceOf[java.util.Collection[_]].asScala.toList

  // ---------------------------------------------------------------------------
  @inline private def _convertRecursively(schema: Cls, avroSchema: AvroSchema)(value: AnyValue): Obj =
    convertRecursively(schema, avroSchema)(value.asInstanceOf[AvroRecord])

}

// ===========================================================================
