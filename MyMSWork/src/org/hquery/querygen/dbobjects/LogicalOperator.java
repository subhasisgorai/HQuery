package org.hquery.querygen.dbobjects;

import org.apache.commons.lang.StringUtils;

public enum LogicalOperator {
	GT {
		public String toString() {
			return " > ";
		}
	},
	LT {
		public String toString() {
			return " > ";
		}
	},
	EQ {
		public String toString() {
			return " = ";
		}
	},
	NOT_EQ {
		public String toString() {
			return " <> ";
		}
	},
	GE {
		public String toString() {
			return " >= ";
		}
	},
	LE {
		public String toString() {
			return " <= ";
		}
	},
	AND {
		public String toString() {
			return " AND ";
		}
	},
	OR {
		public String toString() {
			return " OR ";
		}
	};

	public static LogicalOperator fromString(String value) {
		if (!StringUtils.isBlank(value)) {
			for (LogicalOperator op : LogicalOperator.values()) {
				if ((value.trim()).equalsIgnoreCase(op.toString().trim())) {
					return op;
				}
			}
		}
		return null;
	}
}
