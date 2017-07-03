package hadoop.DataProcess;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 
 * <p>Title:ChineseTokenizer</p>
 * <p>Description: 中文分词,采用es-ik实现
 * </p>
 * @createDate：2013-8-30
 * @author xq
 * @version 1.0
 */
public class ChineseTokenizer {
    /**
     * 
    * @Title: segStr
    * @Description: 返回LinkedHashMap的分词
    * @param @param content
    * @param @return    
    * @return Map<String,Integer>   
    * @throws
     */
    public static List<String> segStr(String content){
        // 分词
        Reader input = new StringReader(content);
        // 智能分词关闭（对分词的精度影响很大）
        IKSegmenter iks = new IKSegmenter(input, true);
        Lexeme lexeme = null;
        String text ="";
      //  long data = 0l;
        List<String> words = new ArrayList<String>();
      //  Set<String> words = new HashSet<String>();
        try {
            while ((lexeme = iks.next()) != null) {
            	text = lexeme.getLexemeText();
         /*       if (words.containsKey(lexeme.getLexemeText())) {
                    data = 	words.get(lexeme.getLexemeText()) + 1;
                  //  if(! iscontainnbspornumber(text))
                    words.put(text,data);
                } else {
                //	if(! iscontainnbspornumber(text))
                    words.put(text, 1L);
                }*/
            	words.add(text);
            }
        }catch(IOException e) {
            e.printStackTrace();
        }
        return words;
    }
    public static boolean iscontainnbspornumber(String str)
	{
		char c[]= str.toCharArray();
		for(int i = 0; i < c.length ; i++){
			if(c[i] >= '0'&& c[i] <= '9'){
				return true;
			}
			if(c[i]  == 'n' && (i + 3) < c.length){
				if(c[i+1] == 'b'&& c[i+2] == 's'&&c[i+3] =='p'){
					return true;
				}		
			}
		}
		return false;
	}
}
