package edu.illinois.ncsa.clowder.extractor;

import org.apache.commons.cli.Options;

public abstract class Extractor {  
  public void commandline(Options options) {}

  public void init() throws ExtractorException {}

	public abstract java.util.Map<String, Object> processFile(java.io.File input) throws ExtractorException;

}
