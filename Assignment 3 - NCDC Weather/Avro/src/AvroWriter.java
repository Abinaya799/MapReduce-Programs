import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class AvroWriter {
    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public static void Writer(Configuration conf, Schema schema, FileSystem fs, Path inputFile, Path outputAvroFile) throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);


        FileStatus[] fileStatus = fs.listStatus(inputFile);

        FSDataOutputStream out = fs.create(outputAvroFile);
        dataFileWriter.create(schema, out);

        for (FileStatus file : fileStatus) {
            System.out.println("Processing file \"" + file.getPath().getName() + "\"");
            FSDataInputStream input_stream = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(input_stream));
            while (input_stream.available() > 0) {
                String line = reader.readLine().replaceAll("\"", "");
                String[] fields = line.split(",");
                if (isNumeric(fields[21]) && isNumeric(fields[23])) {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("station", fields[0]);
                    record.put("date", fields[1]);
                    record.put("Max", Float.parseFloat(fields[21]));
                    record.put("Min", Float.parseFloat(fields[23]));
                    dataFileWriter.append(record);
                }
            }
        }
        dataFileWriter.close();
        fs.close();
    }

}
