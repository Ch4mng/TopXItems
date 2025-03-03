# **README: Running the Spark Application**

## **2. Prerequisites**
Before running the application, ensure you have the following installed:

### **Required Software:**
- **Java 8 or 11** ([Download](https://adoptium.net/))
- **Scala 2.12+** ([Download](https://www.scala-lang.org/download/))
- **Apache Spark 3.x** ([Download](https://spark.apache.org/downloads.html))
- **SBT (Scala Build Tool)** ([Download](https://www.scala-sbt.org/download.html))

### **For Windows Users:**
- Ensure `HADOOP_HOME` is set up correctly if running locally.
  
  setx HADOOP_HOME "C:\hadoop"
  setx PATH "%PATH%;C:\hadoop\bin"
  
- Place `winutils.exe` in `C:\hadoop\bin` if required.

---

## **3. Installation & Setup**
### **Clone the Repository**
git clone https://github.com/your-repo/spark-top-items.git
cd TopXItems

### **Build the Project with SBT**
sbt clean compile

### **Run Unit & Integration Tests**
sbt test

---

## **4. Running the Spark Application**
### **Command-Line Execution:**
To execute the job, provide the **input/output paths** and **Top X parameter**:
sbt run <dataset A path> <dataset B path> <output path> <topX>

#### **Example Execution:**
sbt run data/datasetA.parquet data/datasetB.parquet output/top_items.parquet 5


