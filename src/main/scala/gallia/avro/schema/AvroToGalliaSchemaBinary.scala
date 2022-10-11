package gallia
package avro
package schema

import aptus._
import org.apache.avro.LogicalTypes
  
// ===========================================================================
private object AvroToGalliaSchemaBinary {  

  def bytes(schema: AvroSchema): BasicType = binary1(schema)(BasicType._Binary)
  
  // ---------------------------------------------------------------------------
  def fixed(schema: AvroSchema): BasicType = binary2(schema) {
    Emulation
      .withNameOption(schema.getName)
       match {
        case Some(Emulation.gallia_byte_emulation)  => BasicType._Byte .assert(_ => schema.getFixedSize == 1)
        case Some(Emulation.gallia_short_emulation) => BasicType._Short.assert(_ => schema.getFixedSize == 2)
        case Some(_) | None                         => BasicType._Binary } }

  // ===========================================================================
  private def binary1(schema: AvroSchema)(default: => BasicType): BasicType =
    Option(schema.getLogicalType) match {

      case Some(decimal: LogicalTypes.Decimal) =>
          // TODO: assert name?
          // TODO: t220406143834 - precision always 38? from spark?
          if (decimal.getScale == 0) BasicType._BigInt
          else                       BasicType._BigDec
             
      // ---------------------------------------------------------------------------
      case None => default

      // ---------------------------------------------------------------------------                  
      case Some(value) => unexpected(value, schema)          
    }

  // ===========================================================================
  private def binary2(schema: AvroSchema)(default: => BasicType): BasicType =
    Option(schema.getLogicalType) match {

      case Some(decimal: LogicalTypes.Decimal) =>      
    		  // TODO: assert name?
          // TODO: t220406143834 - precision always 38? from spark?
          if (decimal.getScale == 0) BasicType._BigInt
          else                       BasicType._BigDec
             
      // case Some(decimal: LogicalTypes.Duration) => // TODO: t220406160931
        
      // ---------------------------------------------------------------------------
      case None => default

      // ---------------------------------------------------------------------------                  
      case Some(value) => unexpected(value, schema)          
    }
  
}

// ===========================================================================
