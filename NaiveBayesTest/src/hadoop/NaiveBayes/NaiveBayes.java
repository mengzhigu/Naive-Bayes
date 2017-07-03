package hadoop.NaiveBayes;

//import hadoop.NaiveBayes.NaiveBayes.NaiveBayesMapper.NaiveBayesReducer;

import hadoop.FileProcess.TestFileIsExist;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NaiveBayes extends Configured implements Tool {
	//private static final String OUTPUT_PATH = ClassifierMain.BASE_PATH	+ "ResultOfClassification";
	private static final String PRIOR_PATH = ClassifierMain.BASE_PATH	+ "ClassWordCount/part-r-00000"; 
	private static final String POSTERIOR_PATH = ClassifierMain.BASE_PATH	+ "WordNumInClass/part-r-00000"; 
	private static final String KIND_PATH = ClassifierMain.BASE_PATH+ "TmpKindOfWord/part-r-00000";

	/**
	 * @see 读取path下的所有文件
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public String[] getFileList(String path) throws IOException{
		Configuration conf = new Configuration();

		List<String> files = new ArrayList<String>();
		Path s_path = new Path(path);
		FileSystem fs = 	s_path.getFileSystem(conf);
		if(fs.exists(s_path)){
			for(FileStatus status:fs.listStatus(s_path)){
				files.add(status.getPath().toString());
			}
		}
		fs.close();
		return files.toArray(new String[]{});
	}
	public static class NaiveBayesMapper extends
	Mapper<Text, Text, Text, Text> {

		private Text docID = new Text();
		private Text classAndProbability = new Text();
		private Map<String, Double> priorProbability = new HashMap<String, Double>(); // 类的先验概率

		private Map<String, Integer> tmppriorProbability = new HashMap<String, Integer>(); 
		private Map<String, Integer> tmpposteriorProbability = new HashMap<String, Integer>(); // 单词在具体类中的后验概率_temp
		private double KindOfWord = 0;
		private double wordInClass = 0;
		// 类别数组
		private String[] classGroup ;
		/**
		 * 放大因子
		 */
		private static BigDecimal zoomFactor = new BigDecimal(10);


		/**
		 * 加载先验概率和后验概率
		 */
		@SuppressWarnings({ "deprecation", "rawtypes" })
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			Configuration conf = context.getConfiguration();
			Path prior = new Path(conf.get("PRIOR_PATH"));
			Path posterior = new Path(conf.get("POSTERIOR_PATH"));
			Path kindofword = new Path(conf.get("KIND_PATH"));
			FileSystem fs = FileSystem.get(conf);

			// 从PRIOR_PATH文件读出，并写入map函数tmppriorProbability中
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, prior,conf);
			Text key = new Text();
			IntWritable value = new IntWritable();
			double sum = 0;
			int flag = 0;
			while (reader.next(key, value)) {
				flag++;
				tmppriorProbability.put(key.toString(), value.get());
				sum += value.get();
			}
			reader.close();
			//		System.out.println("sum: "+sum);
			//  计算先验概率，保存至priorProbability（map函数）
			Iterator it = tmppriorProbability.keySet().iterator();
			classGroup = new String [flag];
			while (it.hasNext()) {
				String tmpKey = (String) it.next();
				classGroup[--flag] = tmpKey;
				double tmpValue = tmppriorProbability.get(tmpKey) / sum;
				//			System.out.println("NaiveBayes  priorProbability: "+tmpKey + "  " + tmpValue + "  "+ tmppriorProbability.size());
				priorProbability.put(tmpKey, tmpValue);
			}

			// 从POSTERIOR_PATH文件读出，并写入map函数tmpposteriorProbability中
			SequenceFile.Reader reader1 = new SequenceFile.Reader(fs,posterior, conf);		
			Text key1 = new Text();
			IntWritable value1 = new IntWritable();
			while (reader1.next(key1, value1)) {
				tmpposteriorProbability.put(key1.toString(), value1.get());
			}
			reader1.close();

			//统计单词种类
			SequenceFile.Reader reader2 = new SequenceFile.Reader(fs,kindofword, conf);		
			Text key2 = new Text();
			IntWritable value2 = new IntWritable();
			while(reader2.next(key2, value2)){
				KindOfWord ++;
			}
			//					System.out.println("KindOfWord :"+KindOfWord);
			reader2.close();
			super.setup(context);
		}

		@SuppressWarnings("rawtypes")
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			//	String content = value;//new String(value.getBytes(), 0, value.getLength());
			// 获得文件名和文件上级目录名
			String[] classAndFile = key.toString().split("@");
			String testfileName = classAndFile[1];
			for (String classname : classGroup) {
				//				System.out.println("classifier : "+classname);
				Iterator it = tmppriorProbability.keySet().iterator();
				while (it.hasNext()) {
					String tmpKey = (String) it.next();
					if(tmpKey.equals(classname) ){
						wordInClass =tmppriorProbability.get(tmpKey);
					}
				}
				String[] words = value.toString().split(" ");
				BigDecimal multipleTerm = new BigDecimal(1);
				for(int i = 0; i < words.length; i++){
					if(!words[i].isEmpty()){
						String temkey = words[i];
						String classAndWord = classname + "@" + temkey;
						if(!tmpposteriorProbability.containsKey(classAndWord)){
							tmpposteriorProbability.put(classAndWord, new Integer(0));			
						}
						BigDecimal possternumBD = new BigDecimal(tmpposteriorProbability.get(classAndWord));	
						BigDecimal wordInClassBD = new BigDecimal(wordInClass);	
						BigDecimal KindOfWordBD = new BigDecimal(KindOfWord);	
						BigDecimal xcProb = (possternumBD.add(new BigDecimal(1))).divide(wordInClassBD.add(KindOfWordBD),10, BigDecimal.ROUND_CEILING);					
						multipleTerm = (multipleTerm.multiply(xcProb)).multiply(zoomFactor);
					}
				}
				BigDecimal b2 = new BigDecimal( priorProbability.get(classname).toString());
				multipleTerm = multipleTerm.multiply(b2);
				this.docID.set(testfileName);
				this.classAndProbability.set(classname + "/"+multipleTerm);
				context.write(this.docID, this.classAndProbability);
			}
		}
	}
		public static class NaiveBayesReducer extends
		Reducer<Text, Text, Text, Text> {

			private Text docID = new Text();
			private Text classID = new Text();

			public void reduce(Text key, Iterable<Text> value, Context context)
					throws IOException, InterruptedException {
			
				BigDecimal maxProbability = new BigDecimal (-99999999);
				String maxClass = "";
				// 计算文档属于那一类
				for (Text val : value) {
					String[] tmpVal = val.toString().split("/");
					BigDecimal Probability = new BigDecimal(tmpVal[1]);//(Double.valueOf(tmpVal[1]));
					if (Probability.compareTo(maxProbability) > 0) {
						maxProbability = Probability;
						maxClass = tmpVal[0];
					}
				}
				//		System.out.println("result: "+maxProbability+"  class: "+maxClass );

				this.docID.set(key);
				this.classID.set(maxClass);
				this.classID.set(maxClass + "/" + maxProbability);
				context.write(this.docID, this.classID);
			}
		}
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

		Path outputPath = new Path(ClassifierMain.BASE_PATH	+ "ResultOfClassification");
		if(otherArgs.length != 0){
			outputPath = new Path(otherArgs[0]);
		}
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		conf.set("PRIOR_PATH",PRIOR_PATH);
		conf.set("POSTERIOR_PATH",POSTERIOR_PATH);
		conf.set("KIND_PATH ",KIND_PATH );

		Job job = new Job(conf, "NaiveBayes");
		job.setJarByClass(NaiveBayes.class);
		job.setMapperClass(NaiveBayesMapper.class);
		job.setCombinerClass(NaiveBayesReducer.class);
		job.setReducerClass(NaiveBayesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		if (otherArgs.length == 0 ){
			System.out.println("NaiveBayes : no not write input and output ! ");
			//	SequenceFileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		}
		else{
			for(int i = 1 ; i < otherArgs.length; i ++){
				SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[i]));
			}
		}
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public void main(String args) throws Exception {
		String outputPath =args;//ClassifierMain.BASE_PATH + "ResultOfClassifiaction";//args;
		System.out.println("naive bayes");
		String intPaths = ClassifierMain.BASE_PATH + "InputTest";//BASE_PATH + "InputSequenceTestData";
		NaiveBayes bayes = new NaiveBayes();

		String inputPaths[] = bayes.getFileList(intPaths);

		String[] stemFileNames = new String[inputPaths.length+1];
		stemFileNames[0] = outputPath;

		for (int i = 1; i < inputPaths.length+1 ; i++){
			stemFileNames[i] = inputPaths[i-1]+"/part-r-00000";
		}
		ToolRunner.run(new Configuration(), new NaiveBayes(), stemFileNames);
		//	TestFileIsExist reader = new TestFileIsExist();
		//	reader.readFile(outputPath+"/part-r-00000");
	//	System.exit(res);
	}

}
