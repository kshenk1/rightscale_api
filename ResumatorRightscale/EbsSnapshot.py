
from ResumatorRightScale import ResumatorRightScale
from EbsVolume import EbsVolume
from Server import Server
from collections import OrderedDict
import sys, json, inspect

class EbsSnapshot(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url                = self.base_url + "ec2_ebs_snapshots.%s" % self.resp_format

    def get_snapshots(self, server, server_name):
        ebsv        = EbsVolume(self.account_id, self.username, self.password)

        s_details   = server.get_details({'nickname': server_name})
        href        = s_details[0]['current_instance_href']
        volumes     = ebsv.get_details({'ec2_instance_href': href})

        if not volumes:
            self.logger.warn("No volumes found for %s" % server_name)
            return []

        nickname    = ebsv.get_nickname(volumes)

        if not nickname:
            self.logger.warn("Strange, could not get a volume nickname for %s" % server_name)
            return []

        self.logger.info("Found %d volumes for %s, volume nickname: %s" % (len(volumes), server_name, nickname))

        snaps       = self.get_details({'nickname': str(nickname)}, '~')
        self.logger.info("Located %d snapshots" % len(snaps))
        return snaps

    def get_latest_timestamp(self, server, server_name):
        snaps       = self.get_snapshots(server, server_name)
        timestamps  = []
        if snaps:
            for s in snaps:
                if 'tags' in s and len(s['tags']) > 0:
                    for t in s['tags']:
                        for k, v in t.iteritems():
                            if v.find('timestamp') != -1:
                                timestamps.append(str(v.split('=', 1)[1]))
        if timestamps:
            timestamps = OrderedDict.fromkeys(timestamps).keys()
            return max(timestamps)
        return 0
        