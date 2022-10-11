package gallia
package avro

// ===========================================================================
object AvroGalliaIn {

  def   readToHead(path: AvroFilePath): HeadO = streamToHead(path).force.one
  def streamToHead(path: AvroFilePath): HeadS = {
    val (avroSchema, galliaSchema) = readGalliaSchemasFromAvroFile(path)

    actions.in
      .GenericInputZ( // TODO: t220302091219 - move schema ingest in a proper validation test
          galliaSchema,
          readGalliaDataFromAvroFile(avroSchema, galliaSchema)(path))
      .pipe(heads.Head.inputZ) }

  // ===========================================================================
  def readAObj (path: AvroFilePath): AObj  = readAObjs(path).forceAObj
  def readAObjs(path: AvroFilePath): AObjs = {
      val (avroSchema, galliaSchema) = readGalliaSchemasFromAvroFile(path)

      AObjs(
        galliaSchema,
        readGalliaDataFromAvroFile(avroSchema, galliaSchema)(path)
          .regenerate()
          .consumeAll
          .pipe(Objs.from)) }

    // ---------------------------------------------------------------------------
    private def readGalliaSchemasFromAvroFile(path: AvroFilePath): (AvroSchema, Cls) = {
      val avroSchema = origin.AvroIn.readAvroSchemaFromAvroFile(path)
      avroSchema -> schema.AvroToGalliaSchema.convertRecursively(avroSchema) }

    // ---------------------------------------------------------------------------
    private def readGalliaDataFromAvroFile(avroSchema: AvroSchema, galliaSchema: Cls)(path: AvroFilePath): DataRegenerationClosure[Obj] = {
      new DataRegenerationClosure[Obj] {
        def regenerate: () => aptus.CloseabledIterator[Obj] = () =>
          origin.AvroIn
            .readAvroDataFromAvroFile(path)
            .map(data.AvroToGalliaData.convertRecursively(galliaSchema, avroSchema)) } }

}

// ===========================================================================
