package org.hquery.status;

import org.apache.hadoop.mapred.JobID;

public class Status {
	private JobID jobId;
	private StatusEnum status;
	private float mapCompletePercentage;
	private float reduceCompletePercentage;

	enum StatusEnum {
		PREPARING {
			public String toString() {
				return "Preparing";
			}
		},
		RUNNING {
			public String toString() {
				return "Running";
			}
		},
		SUCCEEDED {
			public String toString() {
				return "Success";
			}
		},
		FAILED {
			public String toString() {
				return "Failed";
			}
		},
		KILLED {
			public String toString() {
				return "Killed";
			}
		},
		UNKNOWN;

		public String toString() {
			return "Unknown";
		}
	}

	public JobID getJobId() {
		return jobId;
	}

	public void setJobId(JobID jobId) {
		this.jobId = jobId;
	}

	public StatusEnum getStatus() {
		return status;
	}

	public void setStatus(StatusEnum status) {
		this.status = status;
	}

	public float getMapCompletePercentage() {
		return mapCompletePercentage;
	}

	public void setMapCompletePercentage(float mapCompletePercentage) {
		this.mapCompletePercentage = mapCompletePercentage;
	}

	public float getReduceCompletePercentage() {
		return reduceCompletePercentage;
	}

	public void setReduceCompletePercentage(float reduceCompletePercentage) {
		this.reduceCompletePercentage = reduceCompletePercentage;
	}

}
