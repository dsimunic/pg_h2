## TLS Certificates

For https to work, provide paths to the certificates through environment variables:

* `CA_CERT_PATH` -- path to the CA certificate. (for example, the value of $(mkcert -CAROOT))
* `CERT_PATH` -- path to the `.pem` certificate file
* `CERT_KEY_PATH` -- path to the certificate private key file


Verify with:

    nghttp -vyn --cert=$CERT_PATH --key=$CERT_KEY_PATH -H':method: OPTIONS'  https://localhost:8443

Or

    client/valid-user-http | hexdump

One quick way to get the cert for localhost is [mkcert](https://github.com/FiloSottile/mkcert).