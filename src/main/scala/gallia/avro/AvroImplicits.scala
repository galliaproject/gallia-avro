package gallia
package avro

import aptus._
import scala.collection.JavaConverters._
import org.apache.avro.Schema.Type
import org.apache.avro.LogicalTypes

// ===========================================================================
private[avro] object AvroImplicits {

  implicit class AvroSchema_(schema: AvroSchema) {
    def isArray : Boolean = schema.getType == Type.ARRAY
    def isRecord: Boolean = schema.getType == Type.RECORD
    
    // ---------------------------------------------------------------------------
    def unionSchemasOpt: Option[Seq[AvroSchema]] =
      if (schema.isUnion) Some(schema.getTypes /* misnomer: getSchemas */.asScala.toList)
      else                None

    def arrayItemsSchemaOpt: Option[AvroSchema] =
      if (schema.isArray) Some(schema.getElementType)
      else                None

    def recordOpt: Option[AvroSchema] = schema.in.someIf(_.isRecord)
    
    // ---------------------------------------------------------------------------
    def decimalLogicalTypeOpt: Option[LogicalTypes.Decimal] =
      Option(schema.getLogicalType) match {
        case Some(decimal: LogicalTypes.Decimal) => Some(decimal)
        case _                                   => None }

    // ===========================================================================  
    def containment: (Container, AvroSchema /* ValueType */) =
      new AvroSchema_(schema)
        .unionSchemasOpt
        .map(_.filterNot(_.isNullable).force.one)
        match {
          case None           => schema.arrayItemsSchemaOpt match {
            case None           => Container._One -> schema
            case Some(items)    => Container._Nes -> items }
          case Some(union)    => new AvroSchema_(union).arrayItemsSchemaOpt match {
            case None           => Container._Opt -> union
            case Some(items)    => Container._Pes -> items } }    
  }

  // ===========================================================================
  implicit class AvroField_(field: AvroField) {
    def forceDecimal: LogicalTypes.Decimal = _contained.decimalLogicalTypeOpt.get
    def forceRecord: AvroSchema            = _contained.recordOpt            .get

    // ---------------------------------------------------------------------------
    private def _contained: AvroSchema = field.schema.containment._2
  }  
}

// ===========================================================================