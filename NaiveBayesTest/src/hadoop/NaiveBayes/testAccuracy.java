package hadoop.NaiveBayes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class testAccuracy  extends Configured implements Tool {
	String TEST_PATH =ClassifierMain.BASE_PATH+"InputTest/";
	String Classifier_PATH = ClassifierMain.BASE_PATH+"ResultOfClassification/part-r-00000";
	String OUTPUT_PATH = ClassifierMain.BASE_PATH+"accuracy";
	static Map<String,String> rightCate = new TreeMap<String,String>();
	static Map<String,String> resultCate = new TreeMap<String,String>();
	
	public String[] getFileList(String path) throws IOException{
		Configuration conf = new Configuration();
		//  FileSystem fs = FileSystem.get(conf);

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
	double computeAccuracy() throws IOException {
		// TODO Auto-generated method stub

		//	rightCate = getMapFromResultFile(classifyResultFile);
		//resultCate = getMapFromResultFile(classifyResultFileNew);
		System.out.println("resultCate.size: "+resultCate.size());
		System.out.println("rightCate.size: "+rightCate.size());
		Set<Map.Entry<String, String>> resCateSet = resultCate.entrySet();
		double rightCount = 0.0;
		for(Iterator<Map.Entry<String, String>> it = resCateSet.iterator(); it.hasNext();){
			Map.Entry<String, String> me = it.next();
			System.out.println("key "+me.getKey());
			System.out.println("resultcate: "+me.getValue());
			System.out.println("rightCate: "+rightCate.get(me.getKey()));
			if(me.getValue().equals(rightCate.get(me.getKey()))){
				rightCount ++;
			}
		}
		//	computerConfusionMatrix(rightCate,resultCate);
		return rightCount / resultCate.size();	
	}
	public static class testAccuracyMapper extends
	Mapper<Text, Text, Text,BytesWritable>  {
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {

			Configuration conf = context.getConfiguration();
			Path result = new Path(conf.get("Classifier_PATH"));

			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, result,conf);
			Text key = new Text();
			Text value = new Text();
	//		System.out.println("setup!");
			while (reader.next(key, value)) {
				String filename = key.toString();
				String[] classAndFile = value.toString().split("/");
				String className = classAndFile[0];			
		//		System.out.println("filename&classname :"+filename+" "+className);
				resultCate.put(filename, className);

			}
			reader.close();
			super.setup(context);
		}

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
//			System.out.println("map !");
			String[] classAndFile = key.toString().split("@");
			String className = classAndFile[0];			
			String fileName = classAndFile[1];
		//	System.out.println("filename&classname :"+fileName+" "+className);
			rightCate.put(fileName, className);
			context.write(key,new BytesWritable(value.getBytes()));//.toString().getBytes()));	
		}

	}
	



	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		conf.set("Classifier_PATH", Classifier_PATH);
		Job job = new Job(conf, "test");
		//	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//	String outpath = new String();
		/*if (otherArgs.length == 0 ){
		System.out.println(" SmallFilesToSequence : without inputpath and outpath !");
		outpath =OUTPUT_PATH;
	}
	else{
		outpath = otherArgs[0];
	}*/
		Path outputPath = new Path(OUTPUT_PATH);

		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setJarByClass(testAccuracy.class);
		job.setMapperClass(testAccuracyMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		//WholeFileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		for(int i = 0 ;i < arg0.length ; i++){
			Path testPath = new Path(arg0[i]);
			SequenceFileInputFormat.addInputPath(job, testPath);
		}

		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		testAccuracy test = new testAccuracy();
		String[] testFiles = test.getFileList(test.TEST_PATH);

		ToolRunner.run(new Configuration(), new testAccuracy(),testFiles);

		System.out.println("accuracy"+test.computeAccuracy());
	}

}