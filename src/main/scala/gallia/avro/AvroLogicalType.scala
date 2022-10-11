package gallia
package avro

import enumeratum.{Enum, EnumEntry}
import org.apache.avro.data.TimeConversions

// ===========================================================================  
sealed trait AvroLogicalType extends EnumEntry {
    val name: String
  }

  // ===========================================================================
  object AvroLogicalType extends Enum[AvroLogicalType] {  
    val values = findValues

    // ---------------------------------------------------------------------------
    case object DECIMAL { val name = "decimal" }

    case object DATE { val name = "date" }

    case object            TIME_MILLIS { val name = "time-millis" }
    case object            TIME_MICROS { val name = "time-micros" }
    
    case object       TIMESTAMP_MILLIS { val name = "timestamp-millis" }
    case object       TIMESTAMP_MICROS { val name = "timestamp-micros" }
    
    case object LOCAL_TIMESTAMP_MILLIS { val name = "local-timestamp-millis" }
    case object LOCAL_TIMESTAMP_MICROS { val name = "local-timestamp-micros" }
    
    // TODO: UUID, duration
    
    // ===========================================================================
    object Data {
      private lazy val                 dateConversion = new TimeConversions.                DateConversion()
      private lazy val           timeMillisConversion = new TimeConversions.          TimeMillisConversion()
      private lazy val           timeMicrosConversion = new TimeConversions.          TimeMicrosConversion()
      private lazy val      timestampMillisConversion = new TimeConversions.     TimestampMillisConversion()
      private lazy val      timestampMicrosConversion = new TimeConversions.     TimestampMicrosConversion()
      private lazy val localTimestampMillisConversion = new TimeConversions.LocalTimestampMillisConversion()
      private lazy val localTimestampMicrosConversion = new TimeConversions.LocalTimestampMicrosConversion()
    
      // ---------------------------------------------------------------------------
      private val NullSchema = null // not actually used by avro (see code)
      private val NullType   = null // not actually used by avro (see code)
      
      // ===========================================================================
      object FromAvro {
        def toBigInt(bytes: Array[Byte])            : BigInt =                          new java.math.BigInteger(bytes)        
        def toBigDec(bytes: Array[Byte], scale: Int): BigDec = new java.math.BigDecimal(new java.math.BigInteger(bytes), scale)
        
        // ---------------------------------------------------------------------------
        def toLocalDate(daysFromEpoch: Int): LocalDate = dateConversion.fromInt(daysFromEpoch, NullSchema, NullType) 
      
        def toLocalTimeViaMillis(daysFromEpoch     : Int ): LocalTime = timeMillisConversion.fromInt (daysFromEpoch,      NullSchema, NullType)
        def toLocalTimeViaMicros(microsFromMidnight: Long): LocalTime = timeMicrosConversion.fromLong(microsFromMidnight, NullSchema, NullType)
        
        def toLocalDateTimeViaMillis(millisFromEpoch: Long): LocalDateTime = localTimestampMillisConversion.fromLong(millisFromEpoch, NullSchema, NullType)
        def toLocalDateTimeViaMicros(microsFromEpoch: Long): LocalDateTime = localTimestampMicrosConversion.fromLong(microsFromEpoch, NullSchema, NullType)
        
        def toInstantViaMillis(millisFromEpoch: Long): Instant = timestampMillisConversion.fromLong(millisFromEpoch, NullSchema, NullType)
        def toInstantViaMicros(microsFromEpoch: Long): Instant = timestampMicrosConversion.fromLong(microsFromEpoch, NullSchema, NullType)             
      }
      
      // ===========================================================================
      object ToAvro {
        
        def fromBigInt(value: BigInt): ByteBuffer = 
          value
            .toByteArray
            .pipe(byteBuffer)
      
        // ---------------------------------------------------------------------------
        def fromBigDec(value: BigDecimal, scale: Int): ByteBuffer =
          value
            .setScale(scale)
            .bigDecimal
            .unscaledValue // from spec: "The byte array must contain the two's-complement representation of the unscaled integer value in big-endian byte order."
            .toByteArray
            .pipe(byteBuffer)
      
        // ===========================================================================
        def fromLocalDate(value: LocalDate): Int = dateConversion.toInt(value, NullSchema, NullType)
        
        def fromLocalTimeMillis(value: LocalTime): Int  = timeMillisConversion.toInt (value, NullSchema, NullType) 
        def fromLocalTimeMicros(value: LocalTime): Long = timeMicrosConversion.toLong(value, NullSchema, NullType)
        
        def fromLocalDateTimeMillis(value: LocalDateTime): Long = localTimestampMillisConversion.toLong(value, NullSchema, NullType)
        def fromLocalDateTimeMicros(value: LocalDateTime): Long = localTimestampMicrosConversion.toLong(value, NullSchema, NullType)
      
        def fromInstantMillis(value: Instant): Long = timestampMillisConversion.toLong(value, NullSchema, NullType)
        def fromInstantMicros(value: Instant): Long = timestampMicrosConversion.toLong(value, NullSchema, NullType)
      }
    }
  } 

// ===========================================================================
