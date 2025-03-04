package com.ctrip.xpipe.redis.proxy.ssl;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;


public class GenerateCertificates {

    private static boolean haLoaded = false;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public static void generateFile() throws Exception {

        if(haLoaded) {
            return;
        }
        haLoaded = true;
        // Generate CA key pair
        KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
        keyPairGen.initialize(2048);
        KeyPair caKeyPair = keyPairGen.generateKeyPair();

        // Generate CA certificate
        X509Certificate caCert = generateCertificate("CN=Test CA", caKeyPair, 365, "SHA256withRSA", true);

        // Generate client key pair
        KeyPair clientKeyPair = keyPairGen.generateKeyPair();

        // Generate client certificate
        X509Certificate clientCert = generateCertificate("CN=Client", clientKeyPair, 365, "SHA256withRSA", false, caCert, caKeyPair.getPrivate());

        // Generate server key pair
        KeyPair serverKeyPair = keyPairGen.generateKeyPair();

        // Generate server certificate
        X509Certificate serverCert = generateCertificate("CN=Server", serverKeyPair, 365, "SHA256withRSA", false, caCert, caKeyPair.getPrivate());

        // Write certificates to files
        writePemFile("CERTIFICATE", "ca.crt", caCert.getEncoded());
        writePemFile("CERTIFICATE", "client.crt", clientCert.getEncoded());
        writePemFile("CERTIFICATE", "server.crt", serverCert.getEncoded());

        // Write private keys to files
        writePemFile("PRIVATE KEY", "pkcs8_client.key", clientKeyPair.getPrivate().getEncoded());
        writePemFile("PRIVATE KEY", "pkcs8_server.key", serverKeyPair.getPrivate().getEncoded());

    }

    private static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm, boolean isCA) throws Exception {
        return generateCertificate(dn, pair, days, algorithm, isCA, null, null);
    }

    private static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm, boolean isCA, X509Certificate issuerCert, PrivateKey issuerKey) throws Exception {
        X500Name issuer = issuerCert == null ? new X500Name(dn) : new X500Name(issuerCert.getSubjectX500Principal().getName());
        X500Name subject = new X500Name(dn);
        BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
        Date notBefore = new Date();
        Date notAfter = new Date(notBefore.getTime() + days * 86400000L);

        X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                issuer, serial, notBefore, notAfter, subject, pair.getPublic()
        );

        certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));
        certBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.digitalSignature));

        ContentSigner signer = new JcaContentSignerBuilder(algorithm).build(issuerKey == null ? pair.getPrivate() : issuerKey);
        return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(certBuilder.build(signer));
    }

    private static void writePemFile(String type, String filename, byte[] encoded) throws IOException {
        Base64.Encoder encoder = Base64.getMimeEncoder(64, new byte[] { '\n' });
        String certDir = System.getProperty("java.io.tmpdir");
        try (FileOutputStream fos = new FileOutputStream(certDir + "/" + filename)) {
            fos.write(("-----BEGIN " + type + "-----\n").getBytes());
            fos.write(encoder.encode(encoded));
            fos.write(("\n-----END " + type + "-----\n").getBytes());
        }
    }

}