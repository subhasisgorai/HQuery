package org.hquery.common.util;

import static org.hquery.common.util.HQueryConstants.COMMA;
import static org.hquery.common.util.HQueryConstants.SPACE_STRING;
import static org.hquery.common.util.HQueryConstants.SEMICOLON;

public class UserPreferences {
	private FileType outputFileType = FileType.DEFAULT;
	private String outputFile;

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public FileType getOutputFileType() {
		return outputFileType;
	}

	public void setOutputFileType(FileType outputFileType) {
		this.outputFileType = outputFileType;
	}

	public enum FileType {
		CSV_TYPE, SPACE_SEPARATED {
			public String getDelimiter() {
				return "\"" + SPACE_STRING + "\"";
			}
		},
		SEMICOLON_SEPARATED {
			public String getDelimiter() {
				return "\"" + SEMICOLON + "\"";
			}
		},
		DEFAULT;
		public String getDelimiter() {
			return "\"" + COMMA + "\"";
		}
	}

}
