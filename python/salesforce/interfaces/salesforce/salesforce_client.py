import pprint
from salesforce_api import login, Salesforce as SalesforceApi
from simple_salesforce import Salesforce
from simple_salesforce.api import DEFAULT_API_VERSION


class SalesforceClient:
    def __init__(self, username, password, login_url, consumer_key, consumer_secret, endpoint):
        self._username = username
        self._password = password
        self._login_url = login_url
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
            # salesforce_api package supports oauth2 login like we used in the gateway-importer lambda but doesn't
            #  appear to have a convenient Apex interface
            sf_login = SalesforceApi(login.oauth2(username=self._username,
                                                  password=self._password,
                                                  client_id=self._consumer_key,
                                                  client_secret=self._consumer_secret,
                                                  instance_url=self._login_url))
            self._access_token = sf_login.connection.access_token
            self._instance_url = sf_login.connection.instance_url
            self._session = sf_login.connection.session  # requests.sessions.Session object
            self._version = sf_login.connection.version
            # simple_salesforce package has the APEX support but not the login method we wanted.  But we have the
            #  details from the above login to satisfy simple_salesforce from the salesforce_api login.
            self._salesforce = Salesforce(session_id=self._access_token, instance_url=self._instance_url)
        return self._salesforce
