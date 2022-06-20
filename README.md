# MapReduce-Programs

This project provides solution for the  following assignment 

[assignment1.pdf](https://github.com/Abinaya799/MapReduce-Programs/files/8939082/assignment1.pdf)


I have made some modifications of my own as follows

1) For assignment 1: The big data file is not available. So i have attached a smaller one. But nevertheless the program will run just fine with large dataset as well.

2) For assignment 3: In newer versions of NCDC data the rows are formatted differently.

    i) in the assignment file it is mentioned that TMAX and TMIN are available as 2 different rows.But in actual data set they are available in the same row.
  
    ii) Station code is also different in the data file available now
  
    iii) As per the assignment, the output was mentioned to be made into seqeunce files, but in this project conversions to avro is also available.
  
    iv) While handling big datasets, it is always best practice to make it compact to reduce the overhead transfer costs. So i have make the climate csv files to a compressed sequence file (the original file was 1.5 gb in size but it was reduced to 300-400 MB)
  
  [For the (iv) Use the program Sequence_Files] 
