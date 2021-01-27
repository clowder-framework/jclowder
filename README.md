


# How to build lib jar
```
cd jclowder
mvn -e clean package -DskipTests
```
the .jar file will be generated under the `target` folder.

# Example Extractor

We provide wordcount java extractor example in this repository. Following is an example code of the WordCount java extractor. This example will allow the user to specify the settings of Rabbitqm and Clowder services, you can get a list of the options by using the `-h` flag on the command line. This will also read some environment variables to initialize the defaults allowing for easy use of this extractor in a Docker container.

From the command line, 
```
cd jclowder
/usr/bin/java -cp target/clowder-extractor-x.x.x-SNAPSHOT.jar example.WordCount --register http://clowderhostip:9000/api/extractors?key=clowderkey --rabbitmqExchange / --rabbitmqURI amqp://guest:guest@rabbitmqhost/%2f --userParam 10
```



```
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
         for( String w : parts) {
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
```

## Initialization
To create a java extractor you should create a new class based on the super class Extractor. This new class can override the functions from the super class Extractor. The `init()` function can initialize class variables from the user input parameters. The `commandline` can allow users to define the customized input parameters. The `processFile` is the function to process the input file, where in this wordcount example, this function will calculate the number of words in the file and return back the map object.

You need to use ExtractorRunner to run the java extractor. This ExtractorRunner class will parse the user input parameters and load the extractor_info.json, in this file it will find
the extractor name, description as well as the key how to register. After ExtractorRunner object has been initialized, you can invoke the `start()` function to start the java extractor. The `start()` function will register this java extractor to the Clowder service and establish the connection to the Rabbitmq services.

## Message Processing
After the successful running, ExtractorRunner will fetch the extraction request from the Rabbitmq and then parse and download the input file. The user's override `processFile` function will be invoked to process the downloaded file. If there is any failure, ExtractorRunner will try 10 times to repeate the processing of the downloaded file. If there is still a failure, ExtractorRunner will move this extraction request to the error queue and process the next available request from the Rabbitmq.

