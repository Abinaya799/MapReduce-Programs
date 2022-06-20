1) This project does the exact same functionality. But here we are converting the input datafile to an avro file with only the fields needed for the computation

2) The output is also an avro file with String key and float value

3) The schema for input avro file is described in ==> weather.avro.scheme

4) The schmea is converted to an class file using the below command:

   java -jar {location of avro-tools-1.11.0.jar} compile schema {location of schema file} {location where you want the class file to be created}
  
