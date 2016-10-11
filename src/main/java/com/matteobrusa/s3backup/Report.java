package com.matteobrusa.s3backup;

public class Report {
	public int count = 0, transferred = 0;
	public long bytes = 0;
	public long time;

	public void incCount() {
		count++;
	}

	public void incTransferred() {
		transferred++;
	}

	public void addBytes(long n) {
		bytes += n;
	}
}
