package gallia

import aptus._

// ===========================================================================
package object avro {
  type Closeabled[T] = aptus.Closeabled[T]
  val  Closeabled    = aptus.Closeabled  

  // ---------------------------------------------------------------------------
  type     Cls = meta.Cls
  lazy val Cls = meta.Cls

  type     Fld = meta.Fld
  lazy val Fld = meta.Fld

  type     SubInfo = meta.SubInfo
  lazy val SubInfo = meta.SubInfo

  type     ValueType = meta.ValueType
  lazy val ValueType = meta.ValueType
  
  type     Container = gallia.reflect.Container
  lazy val Container = gallia.reflect.Container
    
  type     BasicType = gallia.reflect.BasicType
  lazy val BasicType = gallia.reflect.BasicType

  // ===========================================================================
  type AvroSchema = org.apache.avro.Schema
  type AvroField  = org.apache.avro.Schema.Field
  type AvroType   = org.apache.avro.Schema.Type
  type AvroRecord = org.apache.avro.generic.GenericData.Record // (extends GenericRecord)
  
  private[gallia] type CodecFactory = org.apache.avro.file.CodecFactory
  private[gallia] val  DefaultCodec = org.apache.avro.file.CodecFactory.nullCodec
  
  // ---------------------------------------------------------------------------
  type AvroFilePath = aptus.FilePath // .avro extension (schema as JSON + data as binary) - "Object Container Files" in spec (magic header: "Obj1" - TBC)
  type AvscFilePath = aptus.FilePath // .avsc extension (schema as JSON)
  type AvdlFilePath = aptus.FilePath // .avdl extension (schema as IDL)

  // ===========================================================================
  private[avro] val PlaceHolderDoc: String  = null
  private[avro] val PlaceHolderNs : String  = null
  private[avro] val IsError       : Boolean = true
  private[avro] val NoDefaultValue: com.fasterxml.jackson.databind.JsonNode = null

  // ===========================================================================
  implicit class AvroPath (path: String) {
      def readAvro  (): HeadO = AvroGalliaIn .readToHead  (path)
      def streamAvro(): HeadS = AvroGalliaIn .streamToHead(path) }
    
    // ---------------------------------------------------------------------------
    implicit class AvroHeadO(head: HeadO) { def writeAvro(path: AvroFilePath, codecFactory: CodecFactory = DefaultCodec): Unit  = AvroGalliaOut.writeHead(head)(path, codecFactory) }
    implicit class AvroHeadS(head: HeadS) { def writeAvro(path: AvroFilePath, codecFactory: CodecFactory = DefaultCodec): Unit  = AvroGalliaOut.writeHead(head)(path, codecFactory) }    
      
  // ===========================================================================
//TODO: to aptus
  def unexpected[T](value: T, more: Any*): Nothing = { (Seq(value.getClass(), value) ++ more).joinln.p; throw new IllegalStateException() } // convenient in matches; TODO: WeakTypeTag
}

// ===========================================================================