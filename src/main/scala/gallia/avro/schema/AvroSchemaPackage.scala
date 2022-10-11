package gallia
package avro

import aptus._

// ===========================================================================
package object schema {
  private[avro] val array       = "array"
  private[avro] val element     = "element"  
  private[avro] val NoNamespace = ""
  
  // ===========================================================================
  def readSchemaFromAvscFile(path: AvscFilePath): AvroSchema =
    path
      .readFileContent()
      .pipe(readSchemaFromJsonString)

  // ---------------------------------------------------------------------------
  def readSchemaFromAvdlFile(path: AvdlFilePath): AvroSchema =
      new org.apache.avro.compiler.idl.Idl( // unfortunately requires: org.apache.avro:avro-compiler
          new java.io.File(path))
        .RecordDeclaration()

  // ===========================================================================
  def readSchemaFromJsonString(content: JsonString): AvroSchema =
    new org.apache.avro.Schema
      .Parser()
      .parse(content)

  // ---------------------------------------------------------------------------
  def readSchemaFromIdlString(content: String): AvroSchema = {    
    val temp = java.io.File.createTempFile("gallia.", ".avdl") // eg /tmp/gallia.57[...]78.avdl

    // seems to only consume files...
    temp
      .getAbsolutePath
      .tap(content.writeFileContent) // TODO: t220224094305 - security concerns... maybe use reflection to access setDoc instead?
      .pipe(readSchemaFromAvdlFile)   
      .tap { _ => temp.delete() }  
  }
 
  // ===========================================================================
  def writeSchemaJsonString(schema: AvroSchema): JsonString = schema.toString(pretty = false).prettyJson
  
  // ---------------------------------------------------------------------------
  @deprecated("possible?")
  def writeSchemaIdlString (schema: AvroSchema): AvdlFilePath = ??? // TODO: t220224102703 - is it possible with avro-compiler? try manually otherwise?    
  
    // ---------------------------------------------------------------------------
    def writeSchemaJsonFile(schema: AvroSchema, path: AvscFilePath): AvscFilePath = {
      writeSchemaJsonString(schema).writeFileContent(path) }
  
    // ---------------------------------------------------------------------------
    @deprecated("possible?")
    def writeSchemaIdlFile(schema: AvroSchema, path: AvdlFilePath): AvdlFilePath = {
      writeSchemaIdlString(schema).writeFileContent(path) }
  
}

// ===========================================================================