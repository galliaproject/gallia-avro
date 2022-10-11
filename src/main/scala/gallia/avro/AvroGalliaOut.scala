package gallia
package avro

// ===========================================================================
object AvroGalliaOut {

  def writeHead(head: HeadO)(path: AvroFilePath, codecFactory: CodecFactory): Unit = new run.GenericRunnerO(writeAvro(_)(path, codecFactory)).run(head)  
  def writeHead(head: HeadS)(path: AvroFilePath, codecFactory: CodecFactory): Unit = new run.GenericRunnerZ(writeAvro(_)(path, codecFactory)).run(head) 
  
  // ===========================================================================
  def writeAvro(value: AObj)(path: AvroFilePath, codecFactory: CodecFactory = DefaultCodec): Unit = writeAvro(value.inAObjs)(path, codecFactory)
    
  // ---------------------------------------------------------------------------
  def writeAvro(value: AObjs)(path: AvroFilePath, codecFactory: CodecFactory): Unit = {
    val avroSchema = schema.GalliaToAvroSchema(value.c)
    
    origin.AvroOut.writeAvro(path, codecFactory)(avroSchema) { fileWriter =>
      value.z
        .consumeSelfClosing
        .map(data.GalliaToAvroData.convertRecursively(value.c, avroSchema))
        .foreach(fileWriter.append) } }

}

// ===========================================================================
