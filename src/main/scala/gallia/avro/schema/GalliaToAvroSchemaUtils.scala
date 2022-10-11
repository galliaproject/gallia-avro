package gallia
package avro
package schema

import org.apache.avro.Schema

// ===========================================================================
private object GalliaToAvroSchemaUtils {
  val RootNameSpace = "RootNs"
  val DefaultName   = "RootRecord"  
  
  // ---------------------------------------------------------------------------
  val SparkDefaultBigNumPrecision = 38
  val SparkDefaultBigDecScale     = 18
  
  // ---------------------------------------------------------------------------
  // see 220408090819 - let it at least be hackable for now...
  var BigNumPrecision = SparkDefaultBigNumPrecision
  var BigDecScale     = SparkDefaultBigDecScale
  
  // ---------------------------------------------------------------------------
  val NullSchema: Schema = Schema.create(Schema.Type.NULL)

  // ---------------------------------------------------------------------------
  def schema  (tipe   : Schema.Type)          : Schema = Schema.create(tipe)
  def enum    (values: java.util.List[String]): Schema = Schema.createEnum (Schema.Type.ENUM .getName, PlaceHolderDoc, PlaceHolderNs, values)
  def fixed   (size  : Int)                   : Schema = Schema.createFixed(Schema.Type.FIXED.getName, PlaceHolderDoc, PlaceHolderNs, size)
  def nullable(schema: Schema)                : Schema = Schema.createUnion(NullSchema, schema)
  def array   (item  : Schema)                : Schema = Schema.createArray(item)
  def record  (name: String, ns: String)      : Schema = Schema.createRecord(name, PlaceHolderDoc, ns, !IsError)

  def field(name: String)(schema: Schema): Schema.Field = new Schema.Field(name, schema, PlaceHolderDoc, NoDefaultValue)
}

// ===========================================================================
