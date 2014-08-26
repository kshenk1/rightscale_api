
from ResumatorRightScale import ResumatorRightScale
import sys, json, inspect

class EbsVolume(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url                = self.base_url + "ec2_ebs_volumes.%s" % self.resp_format

    def get_details(self, filters={}):
        return self.get_details_with_post_filter(filters)

    def get_nickname(self, volumes):
        ## the nickname of the volumes is always the same for the instance (in our case)
        v_nickname = None
        try:
            v_nickname = volumes[0]['nickname']
            self.logger.info("EBS Volume Nickname: %s" % v_nickname)
        except IndexError:
            self.logger.warn("Unable to get EBS Volume nickname. Number of volumes: %d" % len(volumes))
        return v_nickname