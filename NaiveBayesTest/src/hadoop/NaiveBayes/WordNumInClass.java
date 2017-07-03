package hadoop.NaiveBayes;

import hadoop.FileProcess.MergeFiles;
import hadoop.FileProcess.DataFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordNumInClass extends Configured implements
Tool {
	private static final String OUTPUT_PATH =  ClassifierMain.BASE_PATH+"WordNumInClass";

	/**
	 * @see 读取path下的所有文件
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static String[] getFileList(String path) throws IOException{
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

	static class WordInClassMapper extends Mapper<NullWritable, String, Text, IntWritable>{
		private Text classNameKey = new Text();
		private IntWritable value = new IntWritable(1);
		private String className = " " ;
		// setup在task开始前调用，这里主要是初始化filenamekey
		@Override
		protected void setup(Context context) {

			InputSplit split = context.getInputSplit();
		//	String fileName = ((FileSplit) split).getPath().getName();
			className = ((FileSplit) split).getPath().getParent().getName();
			//	System.out.println("classname; "+className +" filename : "+fileName);
		}
		public void map(NullWritable key,String value, Context context)
				throws IOException, InterruptedException {
			String [] words =value.split(" ");
			for(int i = 0; i < words.length; i++){
				if(!words[i].isEmpty()){
					classNameKey.set(className+"@"+words[i]);
					context.write(classNameKey, this.value);
				}
			}			
		}
	}
	public static class WordInClassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sumOfWordInClass = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable temp : value) {
				sum += temp.get();
			}
			this.sumOfWordInClass.set(sum);
			context.write(key, this.sumOfWordInClass);
		}

	}
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "WordInClass");
		String[] otherArgs = new GenericOptionsParser(conf, arg0).getRemainingArgs();
		String outpath = new String();
		if (otherArgs.length == 0 ){
			System.out.println(" SmallFilesToSequence : without inputpath and outpath !");
			//	outpath =OUTPUT_PATH;
		}
		else{
			outpath = otherArgs[0];
		}
		Path outputPath = new Path(outpath);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setJarByClass(WordNumInClass.class);
		job.setMapperClass(WordInClassMapper.class);
		job.setCombinerClass(WordInClassReducer.class);
		job.setReducerClass(WordInClassReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(DataFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		DataFileInputFormat.addInputPath(job,  new Path(otherArgs[1]));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
		//return 0;
	}
	public void main(String args) throws Exception {
		System.out.println("WordNumInClass");
		//	String fileDir =ClassifierMain.BASE_PATH+"TrainFiles/sample";//"InputSequenceTestData";
		String fileDir =args;

		Configuration conf = new Configuration();
		//	
		Path s_path = new Path(fileDir);
		FileSystem fs = 	s_path.getFileSystem(conf);
		if(fs.exists(s_path)){
			if(fs.isFile(s_path)){
				String[] stemFileNames = new String[2];
				stemFileNames[0] = OUTPUT_PATH;
				stemFileNames[1] = fileDir;
				ToolRunner.run(new Configuration(), new WordNumInClass(),stemFileNames);
				fs.close();
			}
			else{//读取path下的所有文件
				List<String> files = new ArrayList<String>();
				for(FileStatus status:fs.listStatus(s_path)){
					files.add(status.getPath().toString());
				}
				fs.close();
				String Files[] = files.toArray(new String[]{});
				String[] stemFileNames = new String[2];
				//		stemFileNames[0] = OUTPUT_PATH;
				for(int i = 1; i < Files.length+1; i++){
					String fileFullName = Files[i-1];//.getCanonicalPath();
					String fileShortName = new Path( Files[i-1]).getName();
					stemFileNames[0] = OUTPUT_PATH+"Part/"+fileShortName;
					stemFileNames[1] = fileFullName;//+"/part-r-00000";
					ToolRunner.run(new Configuration(), new WordNumInClass(),stemFileNames);
				}
				String[] s = new String[2];
				s[0] = ClassifierMain.BASE_PATH+"WordNumInClass";  //OUTPUT_PATH;
				s[1] =  ClassifierMain.BASE_PATH+"WordNumInClassPart";
				MergeFiles.main(s);
			}
		}
	}
}
