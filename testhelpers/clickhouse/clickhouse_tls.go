// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package clickhousehelper

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path"
	"time"
)

// genCACertificates generates CA and server certificates for TLS testing.
func genCACertificates(savePath string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	// Create directory if it doesn't exist
	certPath := path.Join(cwd, savePath)
	if err := os.MkdirAll(certPath, 0755); err != nil {
		return err
	}

	// Generate CA key
	caKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	// Create CA certificate template
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Test CA",
			Organization: []string{"Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 years
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Create CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Save CA certificate
	caCertFile, err := os.Create(path.Join(certPath, "local_ca.crt"))
	if err != nil {
		return err
	}
	defer caCertFile.Close()

	if err := pem.Encode(caCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}); err != nil {
		return err
	}

	// Save CA key
	caKeyFile, err := os.Create(path.Join(certPath, "local_ca.key"))
	if err != nil {
		return err
	}
	defer caKeyFile.Close()

	if err := pem.Encode(caKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)}); err != nil {
		return err
	}

	// Generate server key
	serverKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return err
	}

	// Create server certificate template
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName:   "localhost",
			Organization: []string{"Test"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(10, 0, 0), // 10 years
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:    []string{"localhost"},
	}

	// Parse CA cert for signing
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return err
	}

	// Create server certificate
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	// Save server certificate
	serverCertFile, err := os.Create(path.Join(certPath, "localnode.crt"))
	if err != nil {
		return err
	}
	defer serverCertFile.Close()

	if err := pem.Encode(serverCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER}); err != nil {
		return err
	}

	// Save server key
	serverKeyFile, err := os.Create(path.Join(certPath, "localnode.key"))
	if err != nil {
		return err
	}
	defer serverKeyFile.Close()

	if err := pem.Encode(serverKeyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)}); err != nil {
		return err
	}

	return nil
}
