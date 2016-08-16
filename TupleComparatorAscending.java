package com.example.outlierdetection;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;
/*
 * @author: Md. Rezaul Karim. I have reused the source code of Mr. Mahmoud Parsian's book 
 *                            Data Algorithm: Recipes for Scaling Up with Hadoop and Spark, O'Reilly Media, 2015
 */

class TupleComparatorAscending implements Comparator<Tuple2<String,Double>>, Serializable {
final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();
   @Override
   public int compare(Tuple2<String,Double> t1,Tuple2<String,Double> t2) {
      return (t1._2.compareTo(t2._2)); // sort based on AVF Score
   }
}
