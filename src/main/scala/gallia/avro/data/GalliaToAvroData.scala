package gallia
package avro
package data

import aptus._
import scala.collection.JavaConverters._
import java.time.format.DateTimeFormatter

// ===========================================================================
object GalliaToAvroData {
  import AvroImplicits._
  import AvroLogicalType.Data.ToAvro._
  
  // ---------------------------------------------------------------------------
  def convertRecursively(galliaSchema: Cls, avroSchema: AvroSchema)(o: Obj): AvroRecord =
      new AvroRecord(avroSchema)
        .tap { rec =>
          galliaSchema
            .fields
            .map { field =>
              galliaSchema.unknownKeys(o).assert(_.isEmpty) // necessary for union types (see 220615165554)

              o .attemptKey(field.key)
                .foreach { value =>
                  rec.put(
                    field.skey,
                    processField(field)(avroSchema)(value) ) } } }

  // ===========================================================================
  private def processField(field: Fld)(avroSchema: AvroSchema)(value: AnyValue): AnyValue =
    field.valueExtractionWithMatching(debug = field.skey)(value) {

      // ---------------------------------------------------------------------------
      nestedGalliaClass => multiple =>
        if (!multiple) value.asInstanceOf[    Obj  ].pipe { convertRecursively(nestedGalliaClass, nestedAvroSchema(avroSchema, field.skey)) }
        else           value.asInstanceOf[Seq[Obj ]].map  { convertRecursively(nestedGalliaClass, nestedAvroSchema(avroSchema, field.skey)) }.asJava } {

      // ---------------------------------------------------------------------------
      bsc => multiple =>
        if (!multiple)  value                     .pipe(_basicValue(bsc))
        else            value.asInstanceOf[Seq[_]].map (_basicValue(bsc)).asJava }

  // ===========================================================================    
  private def _basicValue(basicType: BasicType)(value: AnyValue): AnyValue =
    basicType match {
      case BasicType._Boolean => value
      case BasicType._Double  => value
      case BasicType._Int     => value
      case BasicType._String  => toUtf8(value.asInstanceOf[String]) // necessary?

      // ---------------------------------------------------------------------------
      case _: BasicType._Enm => value.asInstanceOf[EnumValue].stringValue.pipe(avro.data.stringToAvroEnumValue)

      // ---------------------------------------------------------------------------
      // t220311131818 - short/byte: should encode it using zigzag too    
      case BasicType._Byte  => Emulation.gallia_byte_emulation .fixed(value)
      case BasicType._Short => Emulation.gallia_short_emulation.fixed(value)

      case BasicType._Long  => value
      case BasicType._Float => value

      case BasicType._BigInt => fromBigInt(value.asInstanceOf[BigInt])
      case BasicType._BigDec => fromBigDec(value.asInstanceOf[BigDec], scale = 18)

      // ---------------------------------------------------------------------------
      case BasicType._LocalDate      => fromLocalDate          (value.asInstanceOf[LocalDate])
      
      // for these, must favor Micros over Millis for now (see 220407115238)
      case BasicType._LocalTime      => fromLocalTimeMicros    (value.asInstanceOf[LocalTime])
      case BasicType._LocalDateTime  => fromLocalDateTimeMicros(value.asInstanceOf[LocalDateTime])
      case BasicType._Instant        => fromInstantMicros      (value.asInstanceOf[Instant])
      
      // t220407115500 - do an emulation for offset/zoned as well
      case BasicType._OffsetDateTime => formatOffsetDateTime   (value.asInstanceOf[OffsetDateTime])
      case BasicType. _ZonedDateTime => formatZonedDateTime    (value.asInstanceOf[ ZonedDateTime])
      
      // ---------------------------------------------------------------------------
      case BasicType._Binary => value // avro consumes ByteBuffer already
  } 

  // ===========================================================================
  // TODO: t220228161302 - [optim] - cache based on path
  private def nestedAvroSchema(schema: AvroSchema, fieldName: SKey): AvroSchema =  
    schema
      .getFields      
      .asScala
      .find(_.name == fieldName)
      .get
      .schema        
      .containment
      ._2

  // ===========================================================================
  private def toUtf8(value: String): org.apache.avro.util.Utf8 = new org.apache.avro.util.Utf8(value)

  // ---------------------------------------------------------------------------
  private def formatOffsetDateTime(value: OffsetDateTime): String = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(value) // default behavior but more explicit
  private def formatZonedDateTime (value:  ZonedDateTime): String = DateTimeFormatter.ISO_ZONED_DATE_TIME .format(value) // default behavior but more explicit
}

// ===========================================================================
