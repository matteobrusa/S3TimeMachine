package com.matteobrusa.s3backup;

import java.io.File;
import java.nio.file.Paths;
import java.util.Date;

public class SigHelper implements CharSequence {

	public static final char HASH_SEPARATOR = '+';
	public static final char METADATA_SEPARATOR = '_';
	public static final char TSTAMP_SEPARATOR = '-';
	private String sig;

	public SigHelper(String s) {
		this.sig = s;
	}

	public SigHelper(String path, File file) {

		long lastModified = file.lastModified();
		String tstamp = Tools.formatDate(new Date(lastModified));
		sig = path + METADATA_SEPARATOR + file.length() + TSTAMP_SEPARATOR + tstamp;
	}

	public String getOriginalFilePath() {
		return (sig.substring(0, sig.lastIndexOf(METADATA_SEPARATOR)));
	}

	public String stripTstamp() {
		return sig.substring(0, sig.lastIndexOf(TSTAMP_SEPARATOR));
	}

	public String stripHash() {
		return sig.substring(0, sig.lastIndexOf(HASH_SEPARATOR));
	}

	public String appendHash(String hash) {
		return sig + HASH_SEPARATOR + hash;
	}

	// public String stripTstampAppendHash(String hash) {
	// return appendHash(stripTstamp(), hash);
	// }

	public static String removeTstamp(String sig) {
		return sig.substring(0, sig.indexOf(TSTAMP_SEPARATOR)) + sig.substring(sig.indexOf(HASH_SEPARATOR));
	}

	// public static String stripPath(String sig) {
	// return Paths.get(sig).getFileName().toString();
	// }

	public int length() {
		return sig.length();
	}

	public char charAt(int index) {
		return sig.charAt(index);
	}

	public CharSequence subSequence(int start, int end) {
		return sig.substring(start, end);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof String && obj != null) {
			return obj.equals(sig);
		} else
			return super.equals(obj);
	}

	// ----

	public long getFileSize() {

		if (sig.endsWith("/"))
			return 0;
		int p = sig.lastIndexOf(METADATA_SEPARATOR);
		int q = sig.lastIndexOf(HASH_SEPARATOR);

		String s = sig.substring(p + 1, q);

		return new Long(s);
	}

	public String getStoragePath(String hash) {

		int p = sig.lastIndexOf(METADATA_SEPARATOR);
		int q = sig.lastIndexOf(TSTAMP_SEPARATOR);

		String s = (q > p) ? sig.substring(0, q) : sig;

		return Paths.get(s + HASH_SEPARATOR + hash).getFileName().toString();
	}

	@Override
	public String toString() {
		return sig;
	}
}
