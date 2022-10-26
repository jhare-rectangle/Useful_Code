from simple_salesforce import Salesforce
from simple_salesforce.api import DEFAULT_API_VERSION


class SalesforceClient:
    def __init__(self, username, password, consumer_key, consumer_secret, endpoint, domain="login"):
        self._username = username
        self._password = password
        self._domain = domain
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._endpoint = endpoint
        if self._endpoint[0] == '/':
            self._endpoint = self._endpoint[1:]
        self._salesforce = None
        self._access_token = None
        self._instance_url = None
        self._session = None
        self._version = DEFAULT_API_VERSION

    @property
    def salesforce(self):
        return self._salesforce

    def post_data(self, data=None):
        if data is not None:
            self.get_connection()
            if self._salesforce:
                return self._salesforce.apexecute(self._endpoint, method="POST", data=data)
        return None

    def get_connection(self):
        if not self._salesforce:
            self._salesforce = Salesforce(username=self._username, password=self._password, domain=self._domain,
                                          consumer_key=self._consumer_key, consumer_secret=self._consumer_secret)
        return self._salesforce
