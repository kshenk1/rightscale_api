
from ResumatorRightScale import ResumatorRightScale
import sys, json, inspect

class SshKey(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url                = self.base_url + "ec2_ssh_keys.%s" % self.resp_format
        self.ssh_keys           = {}
        self.__set_keys()

    def __set_keys(self):
        """ That's right... there's no index for ssh keys so we have no choice but to list the IDS """
        self.ssh_keys['default-AWS-US-East']    = 297159
        self.ssh_keys['happyhr_prod_ssh']       = 226346001
        self.ssh_keys['resumator_api']          = 310045001
        self.ssh_keys['resumator_github']       = 253637001
        self.ssh_keys['resumator_production']   = 417140001
        self.ssh_keys['resumator_prod_ssh']     = 305349
        self.ssh_keys['resumator_staging']      = 417192001
        
    def get_href(self, filters):
        details = self.get_details(filters)

        try:
            return details['href']
        except KeyError:
            self.logger.error("Cannot access href for ssh key detail")
            return None

    def get_details(self, filters):
        try:
            key_id = self.ssh_keys[filters['aws_key_name']]
        except KeyError:
            self.logger.error("Unable to get ssh key id by: %s" % filters)
            return False

        url         = '/'.join([self.base_url + 'ec2_ssh_keys', str(key_id), "?format=%s" % self.resp_format])

        http_code   = ResumatorRightScale.make_request(self, url)
        contents    = json.loads(self.response_body)

        if http_code == 200 and contents:
            return contents

        self.logger.error("HTTP CODE: %d: Unable to get details for ssh key: %s" % (http_code, filters['aws_key_name']))
        return {}