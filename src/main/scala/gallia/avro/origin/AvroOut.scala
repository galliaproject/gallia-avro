package gallia
package avro
package origin

import org.apache.avro.io.DatumWriter
import org.apache.avro.file.DataFileWriter
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericDatumWriter

// ===========================================================================
object AvroOut {
  
  def writeAvro(path: String, codecFactory: CodecFactory = DefaultCodec)(avroSchema: AvroSchema)(f: DataFileWriter[AvroRecord] => Unit): Unit = {
    val writer: DatumWriter[AvroRecord] = new GenericDatumWriter[AvroRecord](avroSchema)
      val fileWriter = new DataFileWriter[AvroRecord](writer)
        fileWriter.setCodec(codecFactory)
        fileWriter.create(avroSchema, new java.io.File(path))
        f(fileWriter)

    fileWriter.close()
  }  

}

// ===========================================================================
