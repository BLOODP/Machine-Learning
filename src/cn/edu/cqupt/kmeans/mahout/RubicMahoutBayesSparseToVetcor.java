package cn.edu.cqupt.kmeans.mahout;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.map.OpenIntLongHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.vectorizer.TFIDF;

import com.google.common.io.Closeables;


/**
 * 将一个输入文件转换为用于mahout bayes计算的向量
 * @author Guangqin He
 *
 */
public class RubicMahoutBayesSparseToVetcor {
	
	private Path dictionaryFile; //词典文件
	private Path dfCountFile;
	
	OpenObjectIntHashMap<String> dictionary = new OpenObjectIntHashMap<String>();
	OpenIntLongHashMap frequency = new OpenIntLongHashMap();
	Configuration conf;
	
	/**
	 * 
	 * @param dictionaryFile 词典文件
	 * @param dfCountFile 存储有向量的数量 以及每个feature在总体数据中出现的频数
	 * @param conf
	 */
	public RubicMahoutBayesSparseToVetcor(String dictionaryFile,String dfCountFile,Configuration conf){
		this.dictionaryFile = new Path(dictionaryFile);
		this.dfCountFile = new Path(dfCountFile);
		this.conf = conf;
		createDictionary();
	}
	
	/**
	 * 创建词典，将文件中出现的单词或字符对应于一个整数
	 */
	private void createDictionary() {
		
		for (Pair<Writable, IntWritable> pair : new SequenceFileIterable<Writable, IntWritable>(dictionaryFile,true, conf)) {
			dictionary.put(pair.getFirst().toString(), pair.getSecond().get());
		}
		
	}
	
	
	/**
	 * 
	 * @return featureCount vectorCount
	 */
	private long[] count() {
		
		long featureCount = 0;
		long vectorCount = Long.MAX_VALUE;
		
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(dfCountFile, conf)) {
			
			IntWritable key = pair.getFirst();
	        LongWritable value = pair.getSecond();
	        if (key.get() >= 0) {
	        	frequency.put(key.get(), value.get());
	        } else if (key.get() == -1) {
	          vectorCount = value.get();
	        }
	        featureCount = Math.max(key.get(), featureCount);
			
		}
		featureCount++;
		return new long[]{featureCount,vectorCount};

	}
	
	/**
	 * 解析输入文件
	 * @param input
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private StringTuple parseFile(String input) throws FileNotFoundException, IOException {

		File file = new File(input);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		TokenStream stream = analyzer.tokenStream(input, new FileReader(file)); 
		CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
		stream.reset();
		StringTuple document = new StringTuple();
		while(stream.incrementToken()){
			if (termAtt.length() > 0) {
		        document.add(new String(termAtt.buffer(), 0, termAtt.length()));
		      }
		}
		
		
		stream.end();
	    Closeables.close(stream, true);
	    
	    return document;
	}
	
	
	/**
	 * 将输入文件转换为向量
	 * @param input
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private Vector sparseToVector(String input) throws FileNotFoundException, IOException {
		
		StringTuple document = parseFile(input);
		long[] counters = count();
		
		/**     将文件转换为一个 NamedVector对象                      **/
		Vector vector = new RandomAccessSparseVector(1000, document.length());
		for (String term : document.getEntries()) {
			if (!term.isEmpty() && dictionary.containsKey(term)) { // unigram
		          int termId = dictionary.get(term);
		          vector.setQuick(termId, vector.getQuick(termId) + 1);
		        }
		}
		vector = new NamedVector(vector, input);
		
		TFIDF tfidf = new TFIDF();
		for (Vector.Element elment : vector.nonZeroes()) {
			if (!frequency.containsKey(elment.index())) {
		          continue;
		        }
		    long df = frequency.get(elment.index());
		    vector.setQuick(elment.index(), tfidf.calculate((int) elment.get(), (int) df, (int) counters[0], (int) counters[1]));
		}
		
		return vector;
	}
	
	

}
