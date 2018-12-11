#Installation and Run Instructions

This program requires the **Apache Spark** package. If the package is not installed, you can [download](https://spark.apache.org/downloads.html) it at https://spark.apache.org/downloads.html The program was tested on the version **Spark 2.3.2 (Sep 24 2018).**
After downloading the program, unpack \*.tgz archive and copy to the **root** of the disk C, Ex. *C:\spark-2.3.2-bin-hadoop2.7*

If you are using **PyCharm IDE**, you must specify the system paths in the project settings. 
*File -> Settings -> Project -> Project Structure -> Add Content Root*
Choose the following directories and files:
*C:\spark-2.3.2-bin-hadoop2.7\python*
*C:\spark-2.3.2-bin-hadoop2.7\python\lib\py4j-0.10.7-src.zip*

If you are missing **winutils.exe** a hadoop binary, [download](https://github.com/steveloughran/winutils) the **winutils.exe** file and set your hadoop home path. 

1. Download the file at: https://github.com/steveloughran/winutils (The program was tested on the version winutils.exe hadoop-2.7.1.)   
2. Create hadoop folder in Your System ex " *C:*" 
3. Create bin folder in hadoop directory ex : *C:\hadoop\bin* 
4. Paste winutils.exe in bin ex: *C:\hadoop\bin\winuitls.exe* 
5. In **User Variables** in *System Properties -> Advance System Settings.* Create New Variable Name: **HADOOP_HOME** Path: *C:\hadoop\*

Set the environment variable **HADOOP_HOME** to point to the above directory but without **\bin**. 
For example: 
if you copied the **winutils.exe** to *C:\hadoop\bin, set HADOOP_HOME=C:\hadoop* 
if you copied the **winutils.exe** to *C:\spark\hadoop\bin, set HADOOP_HOME=C:\spark\hadoop*

If you receive any errors related to permissions of this folder, use the following commands to set that permissions on that folder:  
List current permissions: *%HADOOP_HOME%\bin\winutils.exe ls \tmp\hive*  
Set permissions: *%HADOOP_HOME%\bin\winutils.exe chmod 777 \tmp\hive*  
List updated permissions: *%HADOOP_HOME%\bin\winutils.exe ls \tmp\hive*  
