package org.hquery.querygen.exception;

public class QueryFormationException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private String exceptionMessage;
	private String errorCode;

	public QueryFormationException() {
	}

	public QueryFormationException(String msg) {
		this.exceptionMessage = msg;
	}

	public QueryFormationException(String code, String errorMessage) {
		this.errorCode = code;
		this.exceptionMessage = errorMessage;
	}

	public QueryFormationException(String code, String msg, Throwable e) {
		this.errorCode = code;
		this.exceptionMessage = msg;
		this.initCause(e);
	}

	public QueryFormationException(String msg, Throwable e) {
		this.exceptionMessage = msg;
		this.initCause(e);
	}

	public void setCause(Throwable e) {
		this.initCause(e);
	}

	@Override
	public String toString() {
		String s = getClass().getName();
		return s + ": " + exceptionMessage;
	}

	public String getMessage() {
		return exceptionMessage;
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}
}
