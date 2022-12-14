# INSERT-UPDATE-DELETE-READ-CRUD-on-Delta-lakes-S3-using-Glue-PySpark-Custom-Jar-Files-Athen


<img width="721" alt="Capture" src="https://user-images.githubusercontent.com/39345855/203443414-bf243bdc-c1fb-413e-a0a4-4bfd3c44bb09.PNG">

### Video Tutorial and steps 
* https://www.youtube.com/watch?v=M0Q9AwnuW-w&feature=youtu.be

# Steps 

*  Upload JAR files on S3 Bucket and then add the path in your glue script as shown in image and copy paste the code and make sure to change base s3 path 

![image](https://user-images.githubusercontent.com/39345855/203443832-43e292ef-2111-4b13-93d4-02d048572762.png)
```
--additional-python-modules  : faker==11.3.0

IN GLUE SCRIPT CHANGE HERE 
base_s3_path = "s3a://YOUR S3 BUCKET"
```




-----------------------------
## """ DELTA Python PySpark Template  """
-----------------------------
* Link: https://github.com/soumilshah1995/INSERT-UPDATE-DELETE-READ-CRUD-on-Delta-lakes-S3-using-Glue-PySpark-Custom-Jar-Files-Athen/blob/main/deltalake-pyspark-template.py


-----------------------------
## """ DELTA Lakes """
-----------------------------
* How to Write | Read | Query Delta lake using AWS Glue and Athena for Queries for Beginners
* Video:  https://www.youtube.com/watch?v=4HUgZksc1eE

* Getting started with Delta lakes Pyspark and AWS Glue (Glue Connector)
* Video: https://www.youtube.com/watch?v=xpU6JPWZ9Pw

* INSERT | UPDATE |DELETE| READ | CRUD |on Delta lake(S3) using Glue PySpark Custom Jar Files & Athena
* Video: https://www.youtube.com/watch?v=M0Q9AwnuW-w

* How do I use Glue to convert existing small parquet files to larger parquet files on Delta Lake
* Video : https://www.youtube.com/watch?v=AGWcGlraqEg&t=455s

* Upsert | Find One and Update in Delta Lake Using Glue Pyspark and Convert Small File into Large File
* Video: https://youtu.be/861mVVgmXw4

-----------------------------
## """HUDI"""
-----------------------------

* Getting started with Apache Hudi with PySpark and AWS Glue #1 Intro
* https://www.youtube.com/watch?v=GhQ6Jr7ZlUM&t=4s

* Different table types in Apache Hudi | MOR and COW | Deep Dive | By Sivabalan Narayanan
* https://www.youtube.com/watch?v=vyEvlt57L-s&t=79s

* Insert | Update | Delete On Datalake (S3) with Apache Hudi and glue Pyspark
* https://www.youtube.com/watch?v=94DPKkzDm-8&t=2s

* Build a Spark pipeline to analyze streaming data using AWS Glue, Apache Hudi, S3 and Athena
* https://www.youtube.com/watch?v=uJI6B4MPmoM
