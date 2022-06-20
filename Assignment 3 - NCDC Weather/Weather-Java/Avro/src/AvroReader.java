import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class AvroReader {
    public static void Reader(Schema schema, Path outputAvroFile, Configuration conf, FileSystem fs) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        FsInput in = new FsInput(outputAvroFile,conf);

        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(in, datumReader);
        GenericRecord user_out;
        while (dataFileReader.hasNext()) {
            user_out = dataFileReader.next();
            System.out.println(user_out);
        }
        dataFileReader.close();

    }

}
