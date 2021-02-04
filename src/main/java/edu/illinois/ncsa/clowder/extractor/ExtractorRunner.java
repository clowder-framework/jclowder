package edu.illinois.ncsa.clowder.extractor;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


public class ExtractorRunner {
  private Log logger = LogFactory.getLog(ExtractorRunner.class);
  private String rabbitmqURI;
  private String rabbitmqExchange;
  private String rabbitmqQueue;
  private Map<String, Object> extractorInfoMap;
  private Extractor extractor;
  private ConnectionFactory factory;
  private DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  private ObjectMapper mapper = new ObjectMapper();
  private String registrationEndpoints;
  private String extractorName;
  private List<String> rabbitmqRoutingKeys;
  private CommandLine cmd;

  public ExtractorRunner(Class<? extends Extractor> extractorClass, String[] args) throws ExtractorException {
    // create instance of the extractor class
    try {
      this.extractor = extractorClass.newInstance();
    } catch (InstantiationException e) {
      throw new ExtractorException("Cannot instantiate extractor object", e);
    } catch (IllegalAccessException e) {
      throw new ExtractorException("Cannot access the extractor class", e);
    }
    // command line options
    Options options = new Options();
    options.addOption("h", "help", false, "show help.");
    options.addOption("r", "register", true, "regstration_endpoints");
    options.addOption(null, "rabbitmqURI", true, "rabbitMQ URI.");
    options.addOption(null, "rabbitmqExchange", true, "rabbitMQ exchange.");
    options.addOption(null, "rabbitmqQueue", true, "rabbitMQ queue.");
    options.addOption(null, "info", true, "extractor_info.json file.");

    // check for additional arguments
    this.extractor.commandline(options);

    // parse command line arguments
    try {
      cmd = new DefaultParser().parse(options, args);
    } catch (ParseException e) {
      throw new ExtractorException("Could not parse command line arguments", e);
    }

    // if -h show help and exit
    if (cmd.hasOption('h')) {
      new HelpFormatter().printHelp(extractorClass.toString(), options);
      System.exit(0);
    }

    // get values from command line or environment variables
    this.rabbitmqURI = cmd.getOptionValue("rabbitmqURI", System.getenv("RABBITMQ_URI"));
    this.rabbitmqExchange = cmd.getOptionValue("rabbitmqExchange", System.getenv("RABBITMQ_EXCHANGE"));
    this.rabbitmqQueue = cmd.getOptionValue("rabbitmqQueue", System.getenv("RABBITMQ_QUEUE"));
    this.registrationEndpoints = cmd.getOptionValue("register", System.getenv("REGISTRATION_ENDPOINTS"));
    String extractorInfo = cmd.getOptionValue("info");

    // load extractor_info.json
    URL context = null;
    if (extractorInfo != null) {
      if (new File(extractorInfo).exists()) {
        try {
          context = new File(extractorInfo).toURI().toURL();
        } catch (MalformedURLException e) {
          throw new ExtractorException("Could not find file.", e);
        }
      }
    } else {
      context = extractorClass.getResource("/extractor_info.json");
    }
    if (context == null) {
      throw new ExtractorException("Could not find extractor_info.json");
    }
    try {
      this.extractorInfoMap = this.mapper.readValue(context, new TypeReference<Map<String, Object>>() {
      });
    } catch (JsonParseException e1) {
      throw new ExtractorException("Invalid JSON in extractor info file.", e1);
    } catch (JsonMappingException e1) {
      throw new ExtractorException("Invalid JSON in extractor info file.", e1);
    } catch (IOException e1) {
      throw new ExtractorException("Cannot read extractor info file.", e1);
    }

    this.extractorName = (String) this.extractorInfoMap.get("name");
    if (rabbitmqQueue == null) {
      rabbitmqQueue = extractorName;
    }
    makeRoutingKeys();

    // setup connection parameters
    factory = new ConnectionFactory();
    try {
      factory.setUri(rabbitmqURI);
    } catch (Exception e) {
      throw new ExtractorException("Cannot configure RabbitMQ connection.", e);
    }
  }

  public String getCommandLineOption(String option) {
    return cmd.getOptionValue(option);
  }

  private void makeRoutingKeys() throws ExtractorException {
    this.rabbitmqRoutingKeys = new ArrayList<String>();
    @SuppressWarnings("unchecked")
    Map<String, List<String>> process = (Map<String, List<String>>) this.extractorInfoMap.get("process");
    for (Entry<String, List<String>> entry : process.entrySet()) {
      for (String mimetype : entry.getValue()) {
        // Replace trailing '*' with '#'
        String mt = mimetype.replaceAll("(\\*$)", "#");
        if (mt.contains("*")) {
          throw new ExtractorException(String.format("Invalid '*' found in rabbitmq_key: %s", mt));
        } else {
          if ("".equals(mt)) {
            this.rabbitmqRoutingKeys.add(String.format("*.%s.#", entry.getKey()));
          } else {
            this.rabbitmqRoutingKeys.add(String.format("*.%s.%s", entry.getKey(), mt.replace("/", ".")));
          }
        }
      }
    }
  }

  public void start() throws ExtractorException {
    // initialize extractor
    this.extractor.init();

    int retry = 10;
    boolean success = false;
    while(retry > 0) {
      try {
        registerExtractor();
        success = true;
        break;
      } catch(IOException e) {
        try {
          Thread.sleep(1000);
        } catch (Exception ex) {}
        retry --;
      }
    }
    if(!success) {
      throw new Error("falied to register the extractor.");
    }
    success = false;
    retry = 10;
    while(retry > 0) {
      try {
        startListening();
        success = true;
        break;
      } catch (Exception e) {
        try {
          Thread.sleep(1000);
        } catch (Exception ex) {}
        retry --;
      }
    }
    if(!success) {
      throw new Error("falied to connect to RabbitMQ.");
    }
  }

  /**
   * Register extractor info with Clowder. This assumes a file called
   * extractor_info.json to be located in the current working folder
   **/
  public void registerExtractor() throws IOException {
    logger.info("Registering extractor...");
    if (this.registrationEndpoints != null) {
      String[] registrationEndpointsList = this.registrationEndpoints.split(",");
      for(String endpoint : registrationEndpointsList) {
        logger.info(endpoint);
        URL url = new URL(endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        OutputStream wr = new DataOutputStream(conn.getOutputStream());
        ObjectMapper extractorMapper = new ObjectMapper();
        extractorMapper.writeValue(wr, this.extractorInfoMap);
        wr.flush();
        wr.close();

        int responseCode = conn.getResponseCode();

        if (responseCode != 200) {
          throw (new IOException("Error uploading metadata [code=" + responseCode + "]"));
        }
      } // end-for
    }
  }

  private void startListening() {
    // connect to rabbitmq
    Connection connection = null;
    try {
      connection = this.factory.newConnection();

      // connect to channel
      final Channel channel = connection.createChannel();

      // declare the exchange
      channel.exchangeDeclare(rabbitmqExchange, "topic", true);

      // declare the queue
      channel.queueDeclare(rabbitmqQueue, true, false, false, null);
      channel.queueDeclare("error." + rabbitmqQueue, true, false, false, null);

      // fetch only one request at once.
      channel.basicQos(1);

      // connect queue and exchange
      for (String routingKey : this.rabbitmqRoutingKeys) {
        channel.queueBind(rabbitmqQueue, rabbitmqExchange, routingKey);
      }

      // start listening
      logger.info("[*] Waiting for messages. To exit press CTRL+C");

      // create listener
      channel.basicConsume(rabbitmqQueue, false, "", new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties header,
            byte[] body) throws IOException {
          onMessage(channel, envelope.getDeliveryTag(), header, new String(body));
        }
      });
    } catch (IOException e) {
      throw new Error("Cannot connect to RabbitMQ host.", e);
    } catch (TimeoutException e) {
      throw new Error("Cannot connect to RabbitMQ host.", e);
    }
  }

  /**
   * Processes a message received over the messagebus. This will first
   * download the file from Clowder, and then process the file sending back
   * the results of the processing.
   * 
   * @param channel
   *            the rabbitMQ channel to send messages over.
   * @param tag
   *            unique id associated with this message.
   * @param header
   *            the header of the incoming message, used for sending
   *            responses.
   * @param body
   *            the actual message to received over the messagebus.
   */
  private void onMessage(Channel channel, long tag, AMQP.BasicProperties header, String body) {
    File inputfile = null;
    String fileid = "";
    String secretKey = "";
    int retry = 10;
    boolean success = false;
    String jobid = "";
    try {
      while(retry > 0) { 
        try {
          @SuppressWarnings("unchecked")
          Map<String, Object> jbody = mapper.readValue(body, Map.class);
          jobid = (String) jbody.get("jobid");
          String host = jbody.get("host").toString();
          fileid = jbody.get("id").toString();
          secretKey = jbody.get("secretKey").toString();
          String intermediatefileid = jbody.get("intermediateId").toString();
          if (!host.endsWith("/")) {
            host += "/";
          }

          statusUpdate(channel, header, jobid, fileid, "Starting file download.");
          // download the file
          inputfile = downloadFile(channel, header, host, secretKey, jobid, fileid, intermediatefileid);

          statusUpdate(channel, header, jobid, fileid, "Processing file.");

          // process file
          Map<String, Object> content = extractor.processFile(inputfile);

          // wrap metadata contents in JSON-LD
          Map<String, Object> jsonld = this.buildJSONLDMetadata(content, "file", fileid);
          // mapper.writeValue((java.io.OutputStream) System.out, jsonld);

          postMetaData(host, secretKey, fileid, jsonld);
          success = true;
          break;
        } catch (Throwable thr) {
          logger.error("Error processing file", thr);
          try {
            statusUpdate(channel, header, jobid, fileid, "Error processing file : " + thr.getMessage());
          } catch (IOException e) {
            logger.warn("Could not send status update.", e);
          }
        } 
        retry--;
      }

      if(!success) {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(header.getCorrelationId())
            .build();
        try {
          channel.basicPublish("", "error." + rabbitmqQueue, props, mapper.writeValueAsBytes(body));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

    } finally {
      try {
        statusUpdate(channel, header, jobid, fileid, "Done");
        // send ack that we are done
        channel.basicAck(tag, false);
      } catch (IOException e) {
        logger.warn("Could not send status update.", e);
      }
      if (inputfile != null) {
        if (!inputfile.delete()) {
          logger.warn("Could not delete inputfile");
        }
      }
    }
  }

  /**
   * Sends a status update back to Clowder about the current status of
   * processing.
   * 
   * @param channel
   *            the rabbitMQ channel to send messages over
   * @param header
   *            the header of the incoming message, used for sending
   *            responses.
   * @param fileid
   *            the id of the file to be processed
   * @param status
   *            the actual message to send back using the messagebus.
   * @throws IOException
   *             if anything goes wrong.
   */
  private void statusUpdate(Channel channel, AMQP.BasicProperties header, String jobid, String fileid, String status)
      throws IOException {
    logger.info("[" + fileid + "] : " + status);

    Map<String, Object> statusreport = new HashMap<String, Object>();
    statusreport.put("job_id", jobid);
    statusreport.put("file_id", fileid);
    statusreport.put("extractor_id", extractorName);
    statusreport.put("status", status);
    statusreport.put("start", dateformat.format(new Date()));

    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(header.getCorrelationId())
        .build();
    channel.basicPublish("", header.getReplyTo(), props, mapper.writeValueAsBytes(statusreport));
  }

  /**
   * Reads the file from the Clowder server and stores it locally on disk.
   * 
   * @param channel
   *            the rabbitMQ channel to send messages over
   * @param header
   *            the header of the incoming message, used for sending
   *            responses.
   * @param host
   *            the remote host to connect to, including the port and
   *            protocol.
   * @param key
   *            the secret key used to access Clowder.
   * @param fileid
   *            the id of the file to be processed
   * @param intermediatefileid
   *            the actual id of the raw file data to process. return the
   *            actual file downloaded from the server.
   * @throws IOException
   *             if anything goes wrong.
   */
  private File downloadFile(Channel channel, AMQP.BasicProperties header, String host, String key, String jobid, String fileid,
      String intermediatefileid) throws IOException {
    statusUpdate(channel, header, jobid, fileid, "Downloading file");

    URL source = new URL(host + "api/files/" + intermediatefileid + "?key=" + key);
    File outputfile = File.createTempFile("Clowder", ".tmp");
    outputfile.deleteOnExit();

    FileUtils.copyURLToFile(source, outputfile);
    return outputfile;
  }

  private Map<String, Object> buildJSONLDMetadata(Map<String, Object> content, String resourceType,
      String resourceID) {
    List<Map<String, String>> contexts = (List<Map<String, String>>) extractorInfoMap.get("contexts");
    Map<String, String> ckcontext = contexts.get(0);
    for (String key : content.keySet()) {
      if (!ckcontext.containsKey(key))
        throw new Error("Key %s is not in extractor_info.json contexts.");
    }

    String contextURL = "https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld";
    String server = "https://clowder.ncsa.illinois.edu/";
    String extractorID = String.format("%sextractors/%s/%s", server, extractorInfoMap.get("name"),
        extractorInfoMap.get("version"));
    Map<String, Object> results = new HashMap<String, Object>();
    Map<String, String> attachedTo = new HashMap<String, String>();
    attachedTo.put("resourceType", resourceType);
    attachedTo.put("id", resourceID);
    Object[] context = {contextURL, contexts.get(0)};
    results.put("@context", context);
    results.put("attachedTo", attachedTo);
    Map<String, String> agent = new HashMap<String, String>();
    agent.put("@type", "cat:extractor");
    agent.put("extractor_id", extractorID);
    results.put("agent", agent);
    results.put("content", content);
    return results;
  }

  /**
   * Post a map as a json message to a Clowder URL. The response is returned
   * as a string.
   * 
   * @param host
   *            the remote host to connect to, including the port and
   *            protocol.
   * @param key
   *            the secret key used to access Clowder.
   * @param fileid
   *            the id of the file whose metadata is uploaded.
   * @param metadata
   *            the actual metadata to upload.
   * @return the reponse of the server as a string.
   * @throws IOException
   *             if anything goes wrong.
   */
  private String postMetaData(String host, String key, String fileid, Map<String, Object> metadata)
      throws IOException {
    URL url = new URL(host + "api/files/" + fileid + "/metadata.jsonld?key=" + key);

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);

    DataOutputStream wr = new DataOutputStream(conn.getOutputStream());

    metadata.put("extractor_id", extractorName);

    mapper.writeValue((java.io.OutputStream) wr, metadata);
    wr.flush();
    wr.close();

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      throw (new IOException("Error uploading metadata [code=" + responseCode + "]"));
    }

    BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();
    logger.debug(response.toString());
    return response.toString();
  }

}
