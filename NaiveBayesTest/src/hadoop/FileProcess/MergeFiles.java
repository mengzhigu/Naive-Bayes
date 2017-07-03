package hadoop.FileProcess;


import hadoop.NaiveBayes.ClassifierMain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MergeFiles extends Configured implements
Tool {
	private static final String OUTPUT_PATH =  ClassifierMain.BASE_PATH+"WordNumInClass";//BASE_PATH + "InputSequenceData";

	/**
	 * @see 读取path下的所有文件
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public  static String[] getFileList(String path) throws IOException{
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
	// 静态内部类，作为mapper
	static class MergeFileMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

		// setup在task开始前调用，这里主要是初始化filenamekey
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "SmallFilesToSequenceFileConverter");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String outpath = new String();
		if (otherArgs.length == 0 ){
			System.out.println(" SmallFilesToSequence : without inputpath and outpath !");
		}
		else{
			outpath = OUTPUT_PATH;
		}
		Path outputPath = new Path(outpath);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setJarByClass(MergeFiles.class);
		job.setMapperClass(MergeFileMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
for(int i = 0; i < otherArgs.length; i++){
	SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[i]));
}
	
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("MergeFiles");
		String[] filepath = MergeFiles.getFileList(args[1]);
		ToolRunner.run(new Configuration(), new MergeFiles(),filepath);

		//	TestFileIsExist reader = new TestFileIsExist();
		//	reader.readFile(OUTPUT_PATH+"/part-r-00000");
		//	System.exit(exitCode);
	}
}
