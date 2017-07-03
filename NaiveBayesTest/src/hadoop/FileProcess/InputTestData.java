package hadoop.FileProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hadoop.NaiveBayes.ClassifierMain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//将test预处理
public class InputTestData extends Configured implements
Tool {
	private static final String OUTPUT_PATH =  ClassifierMain.BASE_PATH+"InputTest";

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

	static class InputTestMapper extends Mapper<NullWritable, String, Text, Text>{
		private Text classNameKey = new Text();

		// setup在task开始前调用，这里主要是初始化classNamekey
		@Override
		protected void setup(Context context) {

			InputSplit split = context.getInputSplit();
			String fileName = ((FileSplit) split).getPath().getName();
			String className = ((FileSplit) split).getPath().getParent().getName();
			classNameKey.set(className+"@"+fileName);
			//	System.out.println("classname; "+className +" filename : "+fileName);
		}
		public void map(NullWritable key,String value, Context context)
				throws IOException, InterruptedException {
			context.write(classNameKey, new Text(value));	
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
		}
		else{
			outpath = otherArgs[0];
		}
		Path outputPath = new Path(outpath);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setJarByClass(InputTestData.class);
		job.setMapperClass(InputTestMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(DataFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		DataFileInputFormat.addInputPath(job,  new Path(otherArgs[1]));
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public void main(String[] args) throws Exception {
		System.out.println("InputTestData");
		String fileDir =ClassifierMain.BASE_PATH+"TestFiles/Sample";
		String[] inputPaths = InputTestData.getFileList(fileDir) ;
		String[] stemFileNames = new String[2];
		//		stemFileNames[0] = OUTPUT_PATH;
		for(int i = 1; i < inputPaths.length+1; i++){
			String fileFullName = inputPaths[i-1];//.getCanonicalPath();
			String fileShortName = new Path( inputPaths[i-1]).getName();
			stemFileNames[0] = OUTPUT_PATH+"/"+fileShortName;
			stemFileNames[1] = fileFullName;//+"/part-r-00000";
			ToolRunner.run(new Configuration(), new InputTestData(),stemFileNames);
		}
	}

}
