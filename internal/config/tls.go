package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// CSRファイル：自分の公開鍵に、コモンネームなどの方法を記述したファイル
// CRTファイル：CSRファイルにSSL証明会社の秘密鍵で暗号化したファイル
// Keyファイル：自分の秘密鍵ファイル

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM(b)
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", cfg.CAFile)
		}
		if cfg.Server {
			// サーバーの場合はクライアント証明書を要求する
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// クライアントの場合はサーバー証明書を検証する
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}

