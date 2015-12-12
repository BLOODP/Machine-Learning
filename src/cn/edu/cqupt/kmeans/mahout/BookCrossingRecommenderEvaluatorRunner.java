package cn.edu.cqupt.kmeans.mahout;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BookCrossingRecommenderEvaluatorRunner {
	
	private static final Logger log = LoggerFactory.getLogger(BookCrossingRecommenderEvaluatorRunner.class);

	private BookCrossingRecommenderEvaluatorRunner() {
	    // do nothing
	  }
	
	public static void main(String[] args) throws IOException, TasteException {
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		File ratingsFile = new File("D:/hadoop/BX-CSV-Dump/BX-Book-Ratings.csv");
		DataModel model = new BookCrossingDataModel(ratingsFile, false);
		evaluator.evaluate(new BookCrossingRecommenderBuilder(), null, model, 0.9, 0.3);
	}
	
}
