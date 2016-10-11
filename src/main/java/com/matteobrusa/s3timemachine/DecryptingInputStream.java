package com.matteobrusa.s3timemachine;

import java.io.IOException;
import java.io.InputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;

import com.matteobrusa.s3timemachine.service.CryptoService;

public class DecryptingInputStream extends InputStream {

	private InputStream source;
	Cipher cipher;
	private CipherInputStream cin;

	public DecryptingInputStream(InputStream source) {
		this.source = source;

		byte[] iv = new byte[16];
		int m;
		try {
			m = source.read(iv);
			if (m != 16) {
				source.close();
				throw new IllegalStateException("could not read full IV");
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		try {
			cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.DECRYPT_MODE, CryptoService.secret, new IvParameterSpec(iv));

		} catch (Exception e) {
			throw new IllegalStateException(e);
		}

		cin = new CipherInputStream(source, cipher);
	}


	@Override
	public int read(byte[] b, int off, int len) throws IOException {

		int n = cin.read(b, off, len);

		return n;
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
		cin.close();
	}
}
