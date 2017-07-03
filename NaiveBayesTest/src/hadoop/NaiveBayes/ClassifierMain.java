package hadoop.NaiveBayes;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import hadoop.FileProcess.InputTestData;
import hadoop.FileProcess.TestFileIsExist;
import hadoop.NaiveBayes.NaiveBayes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ClassifierMain {
	public static final String BASE_PATH = "hdfs://localhost:9000/user/hadoop/CFT/";
	/**
	 * 打印系统当前时间
	 */
	public long printCurrentTime(){
		//	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");//设置日期格式
		Date d = new Date();
		long longtime = d.getTime();
		return longtime;
	}
	/**
	 * 向fileName中写fileContent
	 */
	public boolean write(String fileName, String fileContent) {

		boolean result = false;
		File f = new File(fileName);
		try {
			FileOutputStream fs = new FileOutputStream(f);
			byte[] b = fileContent.getBytes();
			fs.write(b);
			fs.flush();
			fs.close();
			result = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	//上传文件到HDFS
	public boolean put2HDFS(String src , String dst ){ 
		Configuration conf = new Configuration();
		Path dstPath = new Path(dst) ; 
		try{ 
			FileSystem hdfs = dstPath.getFileSystem(conf) ; 
			hdfs.copyFromLocalFile(false, new Path(src), dstPath) ; 
		}catch(IOException ie){ 
			ie.printStackTrace() ; 
			return false ; 
		} 
		return true ; 
	}
	public static void main(String[] args) throws Exception {
		String trainPath =ClassifierMain.BASE_PATH+"TrainFiles/sample";//"InputSequenceTestData";
		String testPath =ClassifierMain.BASE_PATH + "InputTest";
		String OUTPUT_PATH =ClassifierMain.BASE_PATH	+ "ResultOfClassification";
		 String[] stem = new String[2];
		 stem[0]= trainPath;
		 stem[1]=OUTPUT_PATH;
		ClassifierMain main = new ClassifierMain();
		long time1 = main.printCurrentTime();

		//统计训练的文件@单词的个数
		WordNumInClass wordnumInClass = new WordNumInClass();		
		wordnumInClass.main(stem[0]);
		//统计类中的单词数
		ClassWordCount count = new ClassWordCount();
		count.main(null);
		//统计所有出现的单词种类
		CountKind countKind = new CountKind();
		countKind.main(null);
		// 贝叶斯分类
		InputTestData testdata = new InputTestData();
		testdata.main(null);
		NaiveBayes baye =new NaiveBayes();
		baye.main(stem[1]);

		TestFileIsExist reader1 = new TestFileIsExist();
		reader1.readFile(ClassifierMain.BASE_PATH	+ "ResultOfClassification"+"/part-r-00000");
		long time2 = main.printCurrentTime();
	//	main.write("/home/mjq/use_time", "use time: "+(time2 -time1)/1000+"s");
	//	main.put2HDFS("/home/mjq/use_time", ClassifierMain.BASE_PATH+"usetime");
		System.out.println("use time: "+(time2 -time1)/1000+"s");
		
	}
}
