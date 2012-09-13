package org.hquery.querygen.dbobjects;

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
	}

}
