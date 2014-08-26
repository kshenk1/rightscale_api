
from ResumatorRightScale import ResumatorRightScale
import urllib, sys, json, inspect

class Deployment(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url    = self.base_url + "deployments.%s" % self.resp_format

    def get_deployments(self):
        http_code = ResumatorRightScale.make_request(self, self.url)

        content = json.loads(self.response_body)

        if http_code == 200 and content:
            return content
        self.logger.error("HTTP CODE: %d: Unable to get deployments" % http_code)
        return []

    def create(self, data):
        deployment_name = (data['nickname'] if 'nickname' in data else 'unknown')
        if len(data) > 0:
            data = dict(map(lambda (key, value): ("deployment[" + str(key) + "]", value), data.items()))

        http_code = ResumatorRightScale.make_request(self, self.url, "POST", data)

        if http_code == 201:
            self.logger.info("Created Deployment \"%s\"" % deployment_name)
            return True

        self.logger.error("HTTP CODE: %d: Unable to create deployment (%s)" % (http_code, deployment_name))
        return False

    def delete(self, deployment_name):
        filters         = {'nickname': deployment_name}
        deployment_id   = self.get_id(filters)

        if deployment_id == 0:
            self.logger.error("%s: Cannot delete deployment 0" % inspect.stack()[0][3])
            return False

        ## Delete cannot have the .js on the end of deployments...
        url = '/'.join([self.base_url + 'deployments', str(deployment_id)])

        http_code = ResumatorRightScale.make_request(self, url, "DELETE")

        if http_code == 200:
            self.logger.info("Deleted Deployment: %s" % deployment_name)
            return True

        self.logger.error("HTTP CODE: %d: Unable to delete deployment: %s" % (http_code, deployment_name))
        return False






