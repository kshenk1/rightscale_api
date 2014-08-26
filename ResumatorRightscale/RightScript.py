
from ResumatorRightScale import ResumatorRightScale
import sys, json, inspect

class RightScript(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url                = self.base_url + "right_scripts.%s" % self.resp_format

    def get_details(self, filters={}, debug=False):
        return self.get_details_with_post_filter(filters)

        