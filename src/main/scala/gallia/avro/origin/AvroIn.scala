package gallia
package avro
package origin

import scala.collection.JavaConverters._
import org.apache.avro.io.DatumReader
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader

// ===========================================================================
object AvroIn {

  def readAvroSchemaFromAvroFile(path: AvroFilePath): AvroSchema = {
    val reader: DatumReader[AvroRecord] = new GenericDatumReader[AvroRecord]()
    val fileReader = new DataFileReader[AvroRecord](new java.io.File(path), reader)

    fileReader.getSchema
  }

  // ---------------------------------------------------------------------------
  def readAvroDataFromAvroFile(path: AvroFilePath): aptus.CloseabledIterator[AvroRecord] = {
    val reader: DatumReader[AvroRecord] = new GenericDatumReader[AvroRecord]()
    val fileReader = new DataFileReader[AvroRecord](new java.io.File(path), reader)

    aptus.CloseabledIterator.fromPair(fileReader.iterator.asScala, fileReader)
  }

}

// ===========================================================================
