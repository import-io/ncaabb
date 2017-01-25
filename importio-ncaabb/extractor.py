""" Class for interacting with Import.io Extractors """
import os
import json
import requests


class Extractor(object):
    """ Class for handling extractors """
    def __init__(self, extractor_id):
        self._extractor_id = extractor_id
        self._api_key = os.environ['IMPORT_IO_API_KEY']

    def get_json(self):
        """ Fetches the JSON from the latest run """
        url = "https://data.import.io/extractor/{0}/json/latest".format(self._extractor_id)
        querystring = {"_apikey": self._api_key}
        headers = {'Accept': "application/x-ldjson"}
        response = requests.get(url, params=querystring, headers=headers, stream=True)
        list_resp = response.text.splitlines()
        json_resp = list(map(lambda x: json.loads(x), list_resp))
        return json_resp
