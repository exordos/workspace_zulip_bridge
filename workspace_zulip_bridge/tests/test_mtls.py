import pathlib
import unittest.mock

from workspace_zulip_bridge import mtls


def test_client_context_loads_ca_and_bridge_identity():
    context = unittest.mock.Mock()
    ca_file = pathlib.Path("/control/ca.crt")
    certificate_file = pathlib.Path("/control/bridge.crt")
    private_key_file = pathlib.Path("/control/bridge.key")

    with unittest.mock.patch.object(
        mtls.ssl, "create_default_context", return_value=context
    ) as create_default_context:
        result = mtls.client_context(ca_file, certificate_file, private_key_file)

    assert result is context
    create_default_context.assert_called_once_with(cafile=str(ca_file))
    context.load_cert_chain.assert_called_once_with(
        certfile=str(certificate_file),
        keyfile=str(private_key_file),
    )
