package gallia
package avro
package schema

import scala.collection.JavaConverters._
import aptus._
import org.apache.avro.Schema
import org.apache.avro.LogicalTypes

// ===========================================================================
object GalliaToAvroSchema { import GalliaToAvroSchemaUtils._

  def apply(c: Cls): AvroSchema = apply(Nil)(c, RootNameSpace, DefaultName)

    // ---------------------------------------------------------------------------
    private def apply(path: Seq[String])(c: Cls, ns: String, name: String): AvroSchema = {   
      val rec = record(name, ns)  
        c .fields
          .map(avroField(path))
          .toList
          .asJava
          .tap(rec.setFields)
  
      rec
    }
  
  // ===========================================================================
  private def avroField(path: Seq[String])(fld: Fld): Schema.Field = {
    val fieldName =
      fld.skey.require(
        fieldName => !isInvalidName(fieldName),
        fieldName => s"not a valid avro field name: ${fieldName} (consider using gallia's renameRecursively)")

    // ===========================================================================
    fld.info.union.map { subInfo =>
      val container = subInfo.container(fld.isOptional)

      subInfo.valueType match {

        // ---------------------------------------------------------------------------
        case nc: Cls =>
          val (recordName, ns) =
            if (subInfo/*fld*/.isMultiple) (avro.schema.array, (path :+ fieldName).mkString("."))
            else                           (fld.skey,           path              .mkString("."))

          // ---------------------------------------------------------------------------
          val nestedAvroSchema: AvroSchema = apply(path :+ fieldName)(nc, ns, recordName)

          // ---------------------------------------------------------------------------
          container match {
              case Container._One => field(fieldName)(               nestedAvroSchema)
              case Container._Opt => field(fieldName)(nullable(      nestedAvroSchema))
              case Container._Nes => field(fieldName)(         array(nestedAvroSchema))
              case Container._Pes => field(fieldName)(nullable(array(nestedAvroSchema))) }

        // ===========================================================================
        case bsc: BasicType =>
          val basicAvroSchema = basicSchema(bsc)

          container match {
              case Container._One => field(fieldName)(               basicAvroSchema)
              case Container._Opt => field(fieldName)(nullable(      basicAvroSchema))
              case Container._Nes => field(fieldName)(         array(basicAvroSchema))
              case Container._Pes => field(fieldName)(nullable(array(basicAvroSchema))) }
      }}
      match {
      case Seq(sole) => sole
      case multiple =>
        //def nullable(schema: Schema)          : Schema = Schema.createUnion(NullSchema, schema)
        ??? // TODO: 220613170739
    }
  }

  // ===========================================================================
  private def basicSchema(basicType: BasicType): Schema = {
    basicType match {
      case BasicType._String  => schema(Schema.Type.STRING)  
      case BasicType._Boolean => schema(Schema.Type.BOOLEAN) 
      case BasicType._Int     => schema(Schema.Type.INT)     
      case BasicType._Double  => schema(Schema.Type.DOUBLE)  
      
      case BasicType._Long    => schema(Schema.Type.LONG)    
      case BasicType._Float   => schema(Schema.Type.FLOAT)   

      // ---------------------------------------------------------------------------
      case BasicType._Byte    => Emulation.gallia_byte_emulation .fixed       
      case BasicType._Short   => Emulation.gallia_short_emulation.fixed   

      case BasicType._BigInt  => schema(Schema.Type.BYTES).pipe(LogicalTypes.decimal(BigNumPrecision,           0).addToSchema)
      case BasicType._BigDec  => schema(Schema.Type.BYTES).pipe(LogicalTypes.decimal(BigNumPrecision, BigDecScale).addToSchema)

      // ---------------------------------------------------------------------------
      case BasicType._LocalDate      => schema(Schema.Type.INT).pipe(LogicalTypes.date.addToSchema)

      // must favor Micros over Millis for now (see 220407115238)
      case BasicType._LocalTime      => schema(Schema.Type.LONG).pipe(LogicalTypes.          timeMicros.addToSchema)
      case BasicType._LocalDateTime  => schema(Schema.Type.LONG).pipe(LogicalTypes.localTimestampMicros.addToSchema)
      case BasicType._Instant        => schema(Schema.Type.LONG).pipe(LogicalTypes.     timestampMicros.addToSchema)

      // t220407115500 - do an emulation for offset/zoned as well        
      case BasicType._OffsetDateTime => schema(Schema.Type.STRING) 
      case BasicType. _ZonedDateTime => schema(Schema.Type.STRING) 

      // ---------------------------------------------------------------------------
      case BasicType._Binary         => schema(Schema.Type.BYTES) 

      // ---------------------------------------------------------------------------
      case e: BasicType._Enm         => enum(e.stringValues.asJava) }
  }

  // ===========================================================================
  // basically porting logic from (unfortunately) private static method validateName in org.apache.avro.Schema
  private def isInvalidName(value: String): Boolean =
     value == null ||
     value.isEmpty || // TODO: trim? or is " " valid?
    !value.head.pipe { c => Character.isLetter       (c) || c == '_' } ||
    !value.forall    { c => Character.isLetterOrDigit(c) || c == '_' }

}

// ===========================================================================
