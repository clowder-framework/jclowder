package example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.Options;

import edu.illinois.ncsa.clowder.extractor.Extractor;
import edu.illinois.ncsa.clowder.extractor.ExtractorException;
import edu.illinois.ncsa.clowder.extractor.ExtractorRunner;

public class WordCount extends Extractor{
  static ExtractorRunner runner = null;
  
  private int userParam = 0;
  
  public static void main(String[] args) throws Exception {
    WordCount wc = new WordCount();
    runner = new ExtractorRunner(wc.getClass(), args);
    runner.start();
  }
  
  @Override
  public void init() throws ExtractorException {
    this.userParam = Integer.parseInt(runner.getCommandLineOption("userParam"));
  }
  
  @Override
  public void commandline(Options options) {
    options.addOption(null, "userParam", true, "extra input parameter.");
  }
  
  @Override
  public Map<String, Object> processFile(File input) throws ExtractorException {
    System.out.println("userParam: " + userParam);
    int count = 0;
    try {
      FileReader fr = new FileReader(input.getAbsoluteFile());     
      BufferedReader br = new BufferedReader (fr);     
      String line = br.readLine();
      while (line != null) {
         String []parts = line.split(" ");
         for( String w : parts)
         {
           count++;        
         }
         line = br.readLine();
      }         
      System.out.println(count);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new ExtractorException(ex.getMessage());
    }
    
    Map<String, Object> content = new HashMap<String, Object>();
    content.put("characters", (Integer)count);
    
    return content;
  }

}
