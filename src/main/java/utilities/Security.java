package utilities;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Security
{
	public static String hashString(String string)
	{
		MessageDigest messageDigest = null;
		try
		{
			messageDigest = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e) {}
		
		byte[] digest = messageDigest.digest(string.getBytes());
		StringBuffer stringBuffer = new StringBuffer();
		
		for(byte b : digest)
		{
			stringBuffer.append(String.format("%02x", b & 0xff));
		}
		
		return stringBuffer.toString();
	}
}
