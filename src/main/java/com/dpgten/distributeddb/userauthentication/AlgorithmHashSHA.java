package com.dpgten.distributeddb.userauthentication;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class AlgorithmHashSHA {

    public static String convertSHA256Hash(final String enteredString) throws NoSuchAlgorithmException {
        if (enteredString == null) {
            return null;
        }
        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        return String.format("%064x", new BigInteger(1, messageDigest.digest(enteredString.getBytes(StandardCharsets.UTF_8))));
    }

    public static boolean compareSHA256Hash(final String enteredString, final String targetHashFromFile) throws NoSuchAlgorithmException {
        final String enteredStringHash = convertSHA256Hash(enteredString);
        if (enteredStringHash == null || targetHashFromFile == null) {
            return false;
        }
        return enteredStringHash.equals(targetHashFromFile);
    }
}
