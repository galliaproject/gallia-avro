package gallia
package avro
package schema

import aptus._
import scala.collection.JavaConverters._
import org.apache.avro.Schema.Type
import org.apache.avro.LogicalTypes
import gallia.reflect.BasicType._LocalDateTime
  
// ===========================================================================
object AvroToGalliaSchema { import AvroImplicits._  

  def convertRecursively(schema: AvroSchema): Cls =
      schema
        .getFields
        .asScala
        .toList
        .map(convertField)
        .pipe(Cls.apply)            
    
     // ===========================================================================
    private def convertField(field: AvroField): Fld = {
        val (container, avroValueType) = field.schema.containment

        val valueType: ValueType =
          avroValueType.recordOpt match {
            case None               => convertBasicType(avroValueType)
            case Some(nestedRecord) => convertRecursively(nestedRecord) }

        Fld(
            Symbol(field.name),
            meta.Info(container.isOptional, Seq(SubInfo(container.isMultiple, valueType))))
      }
                          
      // ===========================================================================      
      private def convertBasicType(avroValueType: AvroSchema): BasicType =
        avroValueType.getType match {
          case Type.STRING  => BasicType._String
          case Type.DOUBLE  => BasicType._Double
          case Type.BOOLEAN => BasicType._Boolean
          
          // ---------------------------------------------------------------------------
          case Type.INT     => int (avroValueType)(default = BasicType._Int)            
          case Type.LONG    => long(avroValueType)(default = BasicType._Long)

          // ---------------------------------------------------------------------------
          case Type.FLOAT => BasicType._Float

          // ---------------------------------------------------------------------------
          // these may result in _Binary, _BigInt, _BigDec, _Short/_Byte (via convention)
          case Type.FIXED /* fixed bytes */ => AvroToGalliaSchemaBinary.fixed(avroValueType)
          case Type.BYTES                   => AvroToGalliaSchemaBinary.bytes(avroValueType)

          // ---------------------------------------------------------------------------
          case Type.ENUM => avroValueType.getEnumSymbols.asScala.toList.map(EnumValue.apply).pipe(BasicType._Enm.apply)

          // ---------------------------------------------------------------------------
          case Type.RECORD | Type.ARRAY | Type.MAP | Type.UNION | Type.NULL => unexpected(avroValueType)
        }

  // ===========================================================================
  private def int(schema: AvroSchema)(default: => BasicType): BasicType =
    Option(schema.getLogicalType) match {

      case Some(_: LogicalTypes.Date)       => BasicType._LocalDate
      case Some(_: LogicalTypes.TimeMillis) => BasicType._LocalTime

      // ---------------------------------------------------------------------------
      case None => default

      // ---------------------------------------------------------------------------                  
      case Some(value) => unexpected(value, schema)          
    }
  
  // ===========================================================================
  private def long(schema: AvroSchema)(default: => BasicType): BasicType =
    Option(schema.getLogicalType) match {

      case Some(_: LogicalTypes.TimeMicros)           => BasicType._LocalTime

      case Some(_: LogicalTypes.TimestampMillis)      => BasicType._Instant
      case Some(_: LogicalTypes.TimestampMicros)      => BasicType._Instant
      
      case Some(_: LogicalTypes.LocalTimestampMillis) => BasicType._LocalDateTime 
      case Some(_: LogicalTypes.LocalTimestampMicros) => BasicType._LocalDateTime

      // ---------------------------------------------------------------------------
      case None => default

      // ---------------------------------------------------------------------------                  
      case Some(value) => unexpected(value, schema)          
    }
                 
}

// ===========================================================================
