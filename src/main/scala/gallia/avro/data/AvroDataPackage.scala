package gallia
package avro

import org.apache.avro.generic.GenericData

// ===========================================================================
package object data {  
  private val PlaceHolderSchema: AvroSchema = null // doesn't seem to be actually used?
  
  // ---------------------------------------------------------------------------
  def byteArrayToAvroFixed(bytes: Array[Byte])                    : GenericData.Fixed = byteArrayToAvroFixed(bytes, PlaceHolderSchema) // TODO: t220309144728 - what is schema supposed to be used for?
  def byteArrayToAvroFixed(bytes: Array[Byte], schema: AvroSchema): GenericData.Fixed = new GenericData.Fixed(schema, bytes)
  def shortToAvroFixed    (value: Short)                          : GenericData.Fixed = new GenericData.Fixed(PlaceHolderSchema, aptus.aptutils.BinaryUtils.byteBuffer(value).array())

  // ---------------------------------------------------------------------------
  def stringToAvroEnumValue(value: String)                    : GenericData.EnumSymbol = stringToAvroEnumValue(value, PlaceHolderSchema) // TODO: t220309144728 - what is schema supposed to be used for?
  def stringToAvroEnumValue(value: String, schema: AvroSchema): GenericData.EnumSymbol = new GenericData.EnumSymbol(schema, value)
}

// ===========================================================================