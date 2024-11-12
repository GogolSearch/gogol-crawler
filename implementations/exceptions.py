class HTTPError(Exception):
    def __init__(self, status_code, url):
        super().__init__(f"HTTPError sur {url} code de status: {status_code}")
        self.status_code = status_code
        self.url = url
