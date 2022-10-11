package gallia
package avro

import org.apache.avro.Schema

// ===========================================================================
sealed trait Emulation extends EnumEntry {  
    /*protected */def sizeInBytes         : Int
    protected def putValue(value: Any): ByteBuffer => ByteBuffer

    // ===========================================================================
    // meta
    final def fixed: Schema = Schema.createFixed(entryName, PlaceHolderDoc, PlaceHolderNs, sizeInBytes)
      
    // ===========================================================================
    // data
    final def fixed(value: Any): org.apache.avro.generic.GenericData.Fixed =
      java.nio.ByteBuffer
        .allocate(sizeInBytes)
        .pipe(putValue(value))          
        .array
        .pipe(data.byteArrayToAvroFixed)               
  }

  // ===========================================================================
  object Emulation extends Enum[Emulation] {  
    val values = findValues

    // ---------------------------------------------------------------------------
    case object gallia_byte_emulation  extends Emulation { def putValue(value: Any) = _.put     (value.asInstanceOf[Byte]);  def sizeInBytes = 1 }
    case object gallia_short_emulation extends Emulation { def putValue(value: Any) = _.putShort(value.asInstanceOf[Short]); def sizeInBytes = 2 } }

// ===========================================================================