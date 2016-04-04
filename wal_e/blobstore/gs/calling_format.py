from oauth2client.service_account import ServiceAccountCredentials
from gcloud.storage.client import Client
import json

class CallingInfo(object):
    """Encapsulate information used to produce a GCS Connection."""

    def __str__(self):
        return repr(self)

    def connect(self, creds):
        scopes = ['https://www.googleapis.com/auth/devstorage.full_control']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(creds.service_account, scopes=scopes)
        with open(creds.service_account) as data_file:
            data = json.load(data_file)
        client = Client(credentials=credentials, project=data['project_id'])
        return client
