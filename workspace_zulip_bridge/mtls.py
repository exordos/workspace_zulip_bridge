import pathlib
import ssl


def client_context(
    ca_file: pathlib.Path,
    certificate_file: pathlib.Path,
    private_key_file: pathlib.Path,
) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(ca_file))
    context.load_cert_chain(
        certfile=str(certificate_file),
        keyfile=str(private_key_file),
    )
    return context
