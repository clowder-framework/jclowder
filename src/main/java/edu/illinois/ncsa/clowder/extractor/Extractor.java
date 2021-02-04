package edu.illinois.ncsa.clowder.extractor;

import org.apache.commons.cli.Options;

/**
 * Class {@code Extractor} is a abstract class. 
 * Every extractor class needs to have {@code Extractor} as a superclass, and overrides/implements its own functions as needed.
 */
public abstract class Extractor {  
  /**
   * take command-line input parameters.
   * @param options 
   */
  public void commandline(Options options) {}

  /**
   * Prepare the initialization of extractor, e.g., get the value of command-line input parameters.
   * @throws ExtractorException
   */
  public void init() throws ExtractorException {}

  /**
   * Execute the computation on the input file.
   * @param input the input file.
   * @return metadata entries
   * @throws ExtractorException
   */
	public abstract java.util.Map<String, Object> processFile(java.io.File input) throws ExtractorException;

}
