package com.matteobrusa.s3backup;

import java.io.IOException;
import java.io.InputStream;
import java.security.AlgorithmParameters;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;

public class EncryptingInputStream extends InputStream {

	private InputStream source;
	Cipher cipher;
	private byte[] ivs;

	public EncryptingInputStream(InputStream source) {
		this.source = source;
		try {
			cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, CryptoService.secret);
			AlgorithmParameters params = cipher.getParameters();
			ivs = params.getParameterSpec(IvParameterSpec.class).getIV();

			// ivs = null;

		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	byte[] data = new byte[65536];

	@Override
	public int read(byte[] b, int off, int len) throws IOException {

		if (ivs != null) { // first send IV
			int length = ivs.length;
			System.arraycopy(ivs, 0, b, off, length);
			ivs = null;
			return length;
		} else if (cipher == null) { // done
			return -1; // EOF
		} else {
			if (len > data.length)
				len = data.length;

			int readLen = source.read(data, 0, len);

			int chunkLen = 0;
			byte[] chunk = null;
			if (readLen >= 0) {
				chunk = cipher.update(data, 0, readLen);
				chunkLen = chunk.length;
			}

			if (chunkLen > 0) {
				System.arraycopy(chunk, 0, b, off, chunkLen);
				return chunkLen;
			} else {
				source.close();

				byte[] lastChunk;
				try {
					lastChunk = cipher.doFinal();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
				System.arraycopy(lastChunk, 0, b, off, lastChunk.length);
				cipher = null;

				return lastChunk.length;
			}
		}

	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read() throws IOException {
		byte[] b = new byte[1];
		int count = read(b);
		if (count < 0)
			return count;
		else
			return b[0];
	}

	@Override
	public void close() throws IOException {
		source.close();
		super.close();
	}
}
