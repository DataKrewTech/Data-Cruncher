from http.cookies import SimpleCookie
from pyarrow import flight

class DremioClietnAithMiddleFactory(flight.ClientMiddlewareFactory):
    """A factory fhtat created DremioClientAuthMiddleware(s)."""

    def __init__(self) -> None:
        self.call_credential = []

    def start_call(self, info):
        return DremioClietnAithMiddleFactory(self)
    
    def set_call_credential(self, call_credential):
        self.call_credential = call_credential


class DremioClientAuthMiddleWare(flight.ClientMiddleware):
    """
    A ClientMiddleWare that extracts the bearer token from 
    the authorization header returned by Dremio Flight Server Endpoint.

    Parameters
    ----------
    factory: ClientHeaderAuthMiddlewareFactory
        The factory to set call credentials if an authorization header
        with bearer token is returned by the Dremio Server.
    """

    def __init__(self, factory) -> None:
        self.factory = factory

    def received_headers(self, headers):
        auth_header_key = 'authorization'
        authorization_header = []
        for key in headers:
            if key.lower() == auth_header_key:
                authorization_header = headers.get(auth_header_key)

            if not authorization_header:
                raise Exception('Did not receive authorization header back from server')
            self.factory.set_call_credential([b'authorization', authorization_header[0].encode('utf-8')])

class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """
    A factory that creates CookieMiddleware(s).
    """

    def __init__(self) -> None:
        self.cookies = {}

    def start_call(self, info):
        return CookieMiddleWare(self)


class CookieMiddleWare(flight.ClientMiddleware):
    """
    A ClientMiddleWare that received and retransmits cookies.
    For simplicity, this does not auto-expire cookies.

    Parameters
    ----------

    factory: CookieMiddlewareFactory
        The factory containeing the currently cached cookies.
    """

    def __init__(self, factory) -> None:
        self.factory = factory

    def received_headers(self, headers):
        for key in headers:
            if key.lower() == 'set-cookie':
                cookie = SimpleCookie()
                for item in headers.get(key):
                    cookie.load(item)

                self.factory.cookies.update(cookie.items())
    
    def sending_headers(self):
        if self.factory.cookies:
            cookie_string =  '; '.join("{!s}={!s}".format(key, val.value) for (key, val) in self.factory.cookies.items())
            return {b'cookie': cookie_string.encode('utf-8')}        
        return {}

        