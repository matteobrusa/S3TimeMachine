package com.matteobrusa.s3timemachine.service;

import com.matteobrusa.s3timemachine.S3TimeMachine;

public class LoggingService {

	protected void debugInline(String string) {
		if (S3TimeMachine.debug)
			System.out.print(string + " ");

	}

	protected void debug(String string) {
		if (S3TimeMachine.debug)
			System.out.println(string);
	}

	protected void log(String string) {
		System.out.println(string);

	}

}
