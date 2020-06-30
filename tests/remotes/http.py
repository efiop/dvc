# pylint:disable=abstract-method
import pytest

from dvc.path_info import HTTPURLInfo

from .base import Base


class HTTP(Base, HTTPURLInfo):
    @staticmethod
    def get_url(port):  # pylint: disable=arguments-differ
        return f"http://127.0.0.1:{port}"


@pytest.fixture
def http_server(tmp_dir):
    from tests.utils.httpd import PushRequestHandler, StaticFileServer

    with StaticFileServer(handler_class=PushRequestHandler) as httpd:
        yield httpd


@pytest.fixture
def http(http_server):
    yield HTTP(HTTP.get_url(http_server.server_port))


@pytest.fixture
def http_remote(tmp_dir, dvc, http):
    tmp_dir.add_remote(config=http.config)
    yield http
