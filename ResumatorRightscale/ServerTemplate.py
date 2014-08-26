
from ResumatorRightScale import ResumatorRightScale
import sys, json, inspect

class ServerTemplate(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url                = self.base_url + "server_templates.%s" % self.resp_format

    def get_details(self, filters={}, debug=False):
        return self.get_details_with_post_filter(filters)

        