package edu.illinois.ncsa.clowder.extractor;

public class ExtractorException extends Exception {

	private static final long serialVersionUID = -4995731140596115554L;

	public ExtractorException(String message) {
		super(message);
	}

	public ExtractorException(String message, Exception e) {
		super(message, e);
	}
}
