package com.example.outlierdetection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import com.example.SparkSession.UtilityForSparkSession;
import scala.Tuple2;
/*
 *  * @author Md. Rezaul Karim 16/08/2016
 *    I have reused some portion of the source code of Mr. Mahmoud Parsian's book Data Algorithm: Recipes for Scaling Up with Hadoop and Spark, O'Reilly Media, 2015
 *
 */

/**
 * Outlier Detection from Large Scale Categorical Breast Cancer Datasets using Spark 2.0.0  
         
 * Outlier: what it's all about? 
            Outlier detection is one  of the  most important processes of detecting instances  with  unusual behavior
            that occurs in a given system. The discovery of valuable information in the data can be made by doing effective 
            detection of outliers. 
 
 * Why it is so important?             
            From many years, mining for outliers has received significant attention because of its 
            wide applications in various areas such as detecting fraudulent usage of credit cards in banking  sector, 
            unauthorized access in computer networks and  biomedical  field [4]. There  are  various  kinds of effective  approaches to detect outliers in numerical dataset. However, for categorical  dataset  
 *          there are some only few limited approaches [1-4]. To have an outlier you must first have some sort of measurement. 
 *          The reason is that any data point > 3*IQR (Interquartile range) is used to identifiy an outliner. 
 *          However, there is no measurement with categorical data, as I understand. Let's see a very simple example, suppose you have have distributed 2000 Apples and Oranges (1000 each) to 1000 peoples. Now you ask them to choose either 
            an Apple or an Orange and finally found that 999 peoples have chosen Oranges and only one person went for an Apple. 
            We can say that the person who did choose an Apple is an outlier. In this kinds of scenario, we use measurement as 
            a way to detect anomalies. Now with the categorical data, we need to know why choosing an Apple 
            is to be considered as an anomaly detection problem since that data point does not behave as the rest 99.9% of the total population. 
 
 * Motivations:           
            Several initiatives have been taken for making the outlier detection in scalable and faster way [1, 4]. 
            MR-AVF[1] algorithm was implemented using Hadoop based MapReduce framework. However, this algorithm has several 
            issues with I/O, algorithmic complexity, low-latency batch-processing jobs and fully disk based operation. 
            
            Literature [4], proposed 1-parameter outlier detection methods namely ITB-SS and ITB-SP method, which is not scalable either. 
            Among other considerable works includes [2, 3, 5] that are suitable outlier detection in distributed datasets with mixed-type attributes for in-memory processing. 
                        
            In contrary, Apache Spark’s in-memory cluster computing framework that allows user programs to load data into a clusters memory and query it repeatedly, 
            making it well-suited to machine learning algorithms. Spark tries to caches the intermediate data into memory and provides the abstraction of 
            Resilient Distributed Datasets (RDDs), which can be used to overcome these issues by making a difference achieving tremendous 
            success in last few years for handling Big Data with Drug discovery, RDMA, Biological sequence alignment 
            in distributed computing system, over statistical analysis for Network anomaly detection, Historical data, semantic analysis 
            with an increasing demand to discover and explore data for real-time insights, the need to extend MapReduce became apparent and this led to the emergence of Spark. 
                                   
            These facts and successes have motivated me to explore the other areas like Biomedical data analytics for applying Apache Spark Big data analytics. 
            Therefore, in this article, I will try to show how to calculate the outlier for large-scale categorical cancer dataset towards cancer 
            diagnosis using Spark. For the technical implementation, the newly released Spark 2.0.0 which is smarter, faster and lighter will be used for the demonstration. 
            
 * Based on the following research papers: 
 * Paper-1: 
 *       Title: Fast Parallel Outlier Detection for Categorical Datasets using MapReduce
 *       URL: http://www.eecs.ucf.edu/georgiopoulos/sites/default/files/247.pdf
 * Paper-2: 
 *       Title: Analysis of outlier detection in categorical dataset, International Journal of Engineering Research and General Science
                Volume 3, Issue 2,March-April, 2015                                                                                  
 *       URL: http://pnrsolution.org/Datacenter/Vol3/Issue2/85.pdf 
 * Paper-3:
 *       Title: One Pass Outlier Detection for Streaming Categorical Data
 *       URL: http://link.springer.com/chapter/10.1007/978-94-007-7293-9_4
 * Paper-4: 
 *       Title: Information-Theoretic  Outlier  Detection  for  Large-Scale  Categorical  Data,  IEEE  TRANSACTION  on 
                Knowledge and Data Engineering,2011.
         URL: ieeexplore.ieee.org/iel5/69/6419729/06109256.pdf
 *
 * Paper-5: 
 *       Title: Scalable and Efficient Outlier Detection in Large Distributed Data Sets with Mixed-Type Attributes
 *       URL: http://etd.fcla.edu/CF/CFE0002734/Koufakou_Anna_200908_PhD.pdf
 *  
 * Dataset collection:
 *      The Cancer Genome Atlas (TCGA), Catalogue of Somatic Mutations in Cancer (COSMIC), International Cancer Genome Consortium (ICGC) 
 *      are the most widely used cancer and tumor related dataset sources curated from MIT, Harvard and some other institutes. 
 *      However, these datasets are available as very unstructured; therefore, we cannot use them directly to show how to apply large-scale 
 *      machine learning technique on top of them. Rather, we will use simpler datasets that are structured 
 *      and manually curated for the machine learning application development and of course many of them show good classification accuracy. 
 *      For example, the Wisconsin Breast Cancer datasets from the UCI Machine Learning Repository available at http://archive.ics.uci.edu/ml. 
 *      This data was donated by researchers of the University of Wisconsin and includes measurements from digitized images of fine-needle 
 *      aspirate of a breast mass. The values represent characteristics of the cell nuclei present in the digital images.  
  *     
 * Dataset description: 
 *     The breast cancer data includes 569 examples of cancer biopsies, each with 32 features. 
 * 	   One feature is an identification number, another is the cancer diagnosis, and 30 are numeric-valued 
 *     laboratory measurements also called bi-assay. The diagnosis is coded as M to indicate malignant or 
 *     B to indicate benign. The Class distribution is as follows: Benign:458 (65.5%) and Malignant: 241 (34.5%). 
 *     Following this label and classification, we will prepare our training and test dataset accordingly. 
 *     The 30 numeric measurements on the other hand comprise the mean, standard error, and worst (that is, largest) 
 *     value for 10 different characteristics of the digitized cell nuclei. These include:
          •	Radius
          •	Texture
          •	Perimeter
          •	Area
          •	Smoothness
          •	Compactness
          •	Concavity
          •	Concave points
          •	Symmetry
          •	Fractal dimension
       Based on their names, all of the features seem to relate to the shape and size of the cell nuclei. 
       Unless you are an oncologist, you are unlikely to know how each relates to benign or malignant masses. 
       These patterns will be revealed as we continue in the machine learning process.
       
 * Tips: 
       To read more about the Wisconsin breast cancer data, refer to the authors' publication: 
       Nuclear feature extraction for breast tumor diagnosis. IS&T/SPIE 1993 International Symposium on Electronic Imaging: 
       Science and Technology, volume 1905, pp 861-870 by W.N. Street, W.H. Wolberg, and O.L. Mangasarian, 1993.       
 */
public class OutlierDetection { 
    public static void main(String[] args) throws Exception {
    	//Step-2: Create the Spark entry point through a Spark session
    	SparkSession spark = UtilityForSparkSession.mySession();
        final int K = 30;   

        // Step-2: create a spark session and then read input and create the first RDD by reading input (as categorical dataset) and create the first RDD  
        final String inputPath = "breastcancer/input/wdbc.data";
        RDD<String> lines_of_patient_records = spark.sparkContext().textFile(inputPath, 1);  //The Spark Context initiated with Spark session does  not allow JavaRDD but only RDD of String. 
        //Don't worry we will convert the same RDD as JavaRDD when required. 
        lines_of_patient_records.cache(); // Cache it if you have enough memory space; since, we will be using the same in next few steps
        
        // Step-3: perform the map() for each RDD element
        // for each input record of: <record-id><,><data1><,><data2><,><data3><,>...; Where, the PairFunction<T, K, V>	
        //T => Tuple2<K, V>: T = input record, K = categorical data value, V = 1;
        
       /* Sample output should be like this (against 1 record i.e. 1 line): 
        (M,1)
		(17.99,1)
		(10.38,1)
		(122.8,1)
		(1001,1)
		(0.1184,1)
		(0.2776,1)
		(0.3001,1)
		(0.1471,1)
		(0.2419,1)
		(0.07871,1)
		(1.095,1)
		(0.9053,1)
		(8.589,1)
		(153.4,1)
		(0.006399,1)
		(0.04904,1)
		(0.05373,1)
		(0.01587,1)
		(0.03003,1)
		(0.006193,1)
		(25.38,1)
		(17.33,1)
		(184.6,1)
		(2019,1)
		(0.1622,1)
		(0.6656,1)
		(0.7119,1)
		(0.2654,1)
		(0.4601,1)
		(0.1189,1)
        */
        JavaPairRDD<String,Integer> patient = lines_of_patient_records.toJavaRDD().flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
			@Override
            public Iterator<Tuple2<String,Integer>> call(String row) {      
                List<Tuple2<String,Integer>> results = new ArrayList<Tuple2<String,Integer>>();
                // As shown in above figure, a record has the following format: <Patient-id><,><feature1><,><feature3><,><feature3><,>...
                String[] tokens = row.split(","); //i.e. features
                for (int i = 1; i < tokens.length; i++) {
                    results.add(new Tuple2<String,Integer>(tokens[i], 1));
                }
                return results.iterator();
            }
        });
        //ones.saveAsTextFile("output/OutlierRDD");
        
        // Step-4: find frequencies of all categorical data (keep categorical-data as String)
        /*
         * This should produced the output as:
        (M,212)
		(17.99,2)
		(10.38,1)
          .....
		(0.2776,2)
          ....
		(0.1189,2)
         * 
         */
        JavaPairRDD<String, Integer> count_frequency_by_reducebykey = patient.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });  
        //count_frequency_by_reducebykey.saveAsTextFile("output/ReducedRDD");
        
        // Step-5: build an associative array to be used for finding AVF Score
        // public java.util.Map<K,V> collectAsMap() and return the key-value pairs in this RDD to the master as a Map.   
        final Map<String, Integer> AVPMap = count_frequency_by_reducebykey.collectAsMap();    //Don't use HashMap, unless you will miss the duplicated entries    
        
        // Step-6: compute AVF Score using the built associative array
        // for each input record of: <record-id><,><feature_point1><,><feature_point2><,><feature_point3><,>...; Where, the PairFunction<T, K, V>	T => Tuple2<K, V>: T = input record, K = record-id, V = AVF score;
        JavaPairRDD<String,Double> AVF = lines_of_patient_records.toJavaRDD().mapToPair(new PairFunction<String, String, Double>() {
			@Override
            public Tuple2<String,Double> call(String rec) {  
				String[] tokens = rec.split(",");
                String recordID = tokens[0];  //Patient ID who is getting diagnosed
                int sum = 0;
                for (int i = 1; i < tokens.length; i++) {
                    sum += AVPMap.get(tokens[i]);
                }
                double N = (double) (tokens.length -1); //We are excluding the first entry i.e. patientID
                double AVFScore = ((double) sum) / N;
                return new Tuple2<String,Double>(recordID, AVFScore);
            }
        });
        
        //String path = "output/outlier";
		//AVF.saveAsTextFile(path);
        
		// Step-7: take the lowest K AVF scores (Attribute Value Point)
        // java.util.List<T> takeOrdered(int K) and returns the first K (smallest) elements from this RDD using 
        // the natural ordering for T while maintain the order.   
        //Dataset<Row> dataset = toRDD(AVF);
/*        List<Tuple2<String,Double>> outliers = AVF.takeOrdered(K, TupleComparatorDescending.INSTANCE);       
        System.out.println("Descending AVF Score");
        System.out.println("Outliers: ");
        for(Iterator<Tuple2<String, Double>> it = outliers.iterator(); it.hasNext();)
        {
        System.out.println(it.next());
        }*/
        
        /*
         *  Output: => (PaitentID, AVF score)
         *  Descending AVF Score
		    Outliers: 
			(871642,28.225806451612904)
			(923748,28.096774193548388)
			(903483,28.032258064516128)
			(872113,28.0)
			(862722,27.967741935483872)
			(875099,27.967741935483872)
			(925236,27.903225806451612)
			(894047,27.903225806451612)
			(868999,27.870967741935484)
			(925311,27.870967741935484)
			(921092,27.838709677419356)
			(92751,27.806451612903224)
			(9113846,27.64516129032258)
			(91544002,14.096774193548388)
			(865432,13.935483870967742)
			(863031,13.870967741935484)
			(917062,13.838709677419354)
			(8912284,13.806451612903226)
			(897880,13.774193548387096)
			(905502,13.774193548387096)
			(9113239,13.774193548387096)
			(891716,13.774193548387096)
			(912519,13.774193548387096)
			(918192,13.774193548387096)
			(874839,13.741935483870968)
			(86973702,13.741935483870968)
			(9113455,13.741935483870968)
			(903011,13.741935483870968)
			(911384,13.709677419354838)
			(869931,13.709677419354838)
         */
        
        //Ascending order
      
        List<Tuple2<String,Double>> outliers = AVF.takeOrdered(K, TupleComparatorAscending.INSTANCE);       
        System.out.println("Ascending AVF Score");
        System.out.println("Outliers: ");
        for(Iterator<Tuple2<String, Double>> it = outliers.iterator(); it.hasNext();)
        {
        System.out.println(it.next());
        }           
        // Step-8: Stop the Spark session
        spark.stop();
    }
}

