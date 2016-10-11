package com.matteobrusa.s3backup;

public class LoggingService {

	protected void debugInline(String string) {
		if (S3Backup.debug)
			System.out.print(string + " ");

	}

	protected void debug(String string) {
		if (S3Backup.debug)
			System.out.println(string);
	}

	protected void log(String string) {
		System.out.println(string);

	}

}
