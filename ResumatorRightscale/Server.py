
from ResumatorRightScale import ResumatorRightScale
from ServerTemplate import ServerTemplate
from SecurityGroup import SecurityGroup
from SshKey import SshKey
from RightScript import RightScript
import urllib, sys, json, inspect, subprocess, itertools

class Server(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url                = self.base_url + "servers.%s" % self.resp_format

    def get_settings(self, server_id):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        # Ugh... you can't use servers.js here, you can only get back json by setting the format on the query string.
        url         = '/'.join([self.base_url + 'servers', str(server_id), "settings?format=%s" % self.resp_format])
        http_code   = ResumatorRightScale.make_request(self, url)
        contents    = json.loads(self.response_body)

        if http_code == 200 and contents:
            return contents

        self.logger.error("HTTP CODE: %d: Unable to pull settings for server id: %d" % (http_code, server_id))
        return {}

    def add_security_groups(self, server_name, security_groups):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        server_id = self.get_id({'nickname': server_name})
        url = '/'.join([self.base_url + 'servers', str(server_id)])

        ## aight... this is horrible and ugly but it's the ONLY way I could assign more than one
        ## security group to an instance.
        ## Even the "rs-update-server-security-groups.sh" script didn't work. Anytime I added another
        ## security group, it removed the first one that I added... so I was always left with the last
        ## security group added to the instance.
        ## This led me to this conclusion: 
        ## http://support.rightscale.com/12-Guides/03-Rightscale_API/RightScale_API_Examples/Server_Operations/Update_a_Server_Security_Group
        groups = []
        sg      = SecurityGroup(self.account_id, self.username, self.password)

        for s_group in security_groups:
            groups.append('-d')
            groups.append("server[ec2_security_groups_href][]=%s" % sg.get_href({'aws_group_name': s_group}))

            self.logger.info("Adding Security Group: %s" % s_group)

        curl_opts   = [['curl', '-s', '-H', 'X-API-VERSION: %s' % self.api_version, '--user', '%s:%s' % (self.username, self.password), '--request', 'PUT'], groups, [url]]
        curl_opts   = list(itertools.chain(*curl_opts))

        sub_call    = subprocess.Popen(curl_opts)
        sub_call.wait()

        ## if we didn't return a good status, then we didn't attach the security groups and something will be b0rk3n
        if sub_call.returncode == 0:
            self.logger.info("Attaching security groups successful.")
            return True

        self.logger.error("Unable to attach security groups! Return Code: %s" % sub_call.returncode)
        return False

    def run_recipe(self, server_name, recipe, inputs={}):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': server_name}
        server_id   = self.get_id(filters)
        url         = '/'.join([self.base_url + 'servers', str(server_id), 'run_executable'])

        post_data   = {'recipe': recipe}
        post_data   = dict(map(lambda (key, value): ("server[" + str(key) + "]", value), post_data.items()))

        if inputs:
            inputs      = dict(map(lambda (key, value): ("server[parameters][" + str(key) + "]", value), inputs.items()))
            post_data   = dict(post_data.items() + inputs.items())

        self.logger.debug("post data: %s" % post_data)
        self.logger.info("Running recipe: %s on %s" % (recipe, server_name))

        http_code   = ResumatorRightScale.make_request(self, url, "POST", post_data)
        
        if http_code == 201:
            self.logger.info("Successfully executed %s on %s" % (recipe, server_name))

            return True

        self.logger.error("HTTP CODE: %d: %s failed on %s" % (http_code, recipe, server_name))
        return False

    def run_rightscript(self, server_name, script_data, inputs={}):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': server_name}
        server_id   = self.get_id(filters)
        url         = '/'.join([self.base_url + 'servers', str(server_id), 'run_script'])

        rs          = RightScript(self.account_id, self.username, self.password)

        post_data   = {'right_script_href': rs.get_href(script_data)}
        post_data   = dict(map(lambda (key, value): ("server[" + str(key) + "]", value), post_data.items()))

        if inputs:
            inputs      = dict(map(lambda (key, value): ("server[parameters][" + str(key) + "]", value), inputs.items()))
            post_data   = dict(post_data.items() + inputs.items())

        self.logger.debug("post data: %s" % post_data)
        self.logger.info("Running rightscript: \"%s\" on %s" % (', '.join(["%s: %s" % (key, val) for (key, val) in script_data.items()]), server_name))

        http_code   = ResumatorRightScale.make_request(self, url, "POST", post_data)
        
        if http_code == 201:
            self.logger.info("Successfully executed %s on %s" % (script_data['name'], server_name))

            return True

        self.logger.error("HTTP CODE: %d: %s failed on %s" % (http_code, script_data['name'], server_name))
        return False

    def update_input(self, server_name, params):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        if len(params) > 0:

            self.logger.info("~" * 50)
            self.logger.info("Updating parameters for server: %s" % server_name)
            self.logger.info("    ~~~~ PARAMETERS ~~~~")
            for k, v in params.items():
                self.logger.info("%s: %s" % (k, v))
            self.logger.info("~" * 50)
            
            filters     = {'nickname': server_name}
            server_id   = self.get_id(filters)
            update_data = dict(map(lambda (key, value): ("server[parameters][" + str(key) + "]", value), params.items()))

            url = '/'.join([self.base_url + 'servers', str(server_id)])

            http_code = ResumatorRightScale.make_request(self, url, "PUT", update_data)

            if http_code == 204:
                self.logger.info("%s: Updated inputs" % server_name)
                return True
            
            self.logger.error("HTTP CODE: %d: %s: Unable to update inputs!" % (http_code, server_name))
            return False

        self.logger.error("No params sent")
        return False

    def create(self, server_data, cloud_id=1):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        server_name     = (server_data['nickname'] if 'nickname' in server_data else 'unknown')

        st              = ServerTemplate(self.account_id, self.username, self.password)
        ssh             = SshKey(self.account_id, self.username, self.password)
        template_data   = server_data.pop('server_template', None)
        key_data        = server_data.pop('ssh_key', None)
        security_groups = server_data.pop('security_groups', None)

        server_data['server_template_href'] = st.get_href(template_data)
        server_data['ec2_ssh_key_href']     = ssh.get_href(key_data)

        _ebs_optimized                      = ('Yes' if 'ebs_optimized' in server_data and (server_data['ebs_optimized'] == 1 or server_data['ebs_optimized'] == 'true') else 'No')

        self.logger.info("~" * 50)
        self.logger.info("      Creating Server: %s" % server_name)
        self.logger.info("      Server Template: %s" % ', '.join(["%s: %s" % (key, val) for (key, val) in template_data.items()]))
        self.logger.info("      Security Groups: %s" % ', '.join(security_groups))
        self.logger.info("        Instance Type: %s" % server_data['instance_type'])
        #self.logger.info("              Pricing: %s" % server_data['pricing'])
        self.logger.info("        EBS Optimized: %s" % _ebs_optimized)
        self.logger.info("              SSH Key: %s" % key_data['aws_key_name'])
        self.logger.info("             Cloud Id: %d" % cloud_id)
        self.logger.info("~" * 50)

        server_data = dict(map(lambda (key, value): ("server[" + str(key) + "]", value), server_data.items()))

        if cloud_id != 1:
            self.logger.warn("Deviating from default cloud id of 1!")

        server_data['cloud_id'] = cloud_id

        self.logger.debug("Server post data: %s" % server_data)

        http_code = ResumatorRightScale.make_request(self, self.url, "POST", server_data)

        if http_code == 201:
            self.logger.info("Created server '%s'" % server_name)
            return self.add_security_groups(server_name, security_groups)
        
        self.logger.error("HTTP CODE: %d: Unable to create '%s'" % (http_code, server_name))
        return False

    def launch(self, server_name):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': server_name}
        server_id   = self.get_id(filters)
        url         = '/'.join([self.base_url + 'servers', str(server_id), 'start'])

        self.logger.info("Launching Server: %s" % server_name)

        http_code   = ResumatorRightScale.make_request(self, url, "POST")

        if http_code == 201:
            self.logger.info("%s launched" % server_name)
            return True

        self.logger.error("HTTP CODE: %d: Server launch failed for: %s" % (http_code, server_name))
        return False

    def terminate(self, server_name):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': server_name}
        server_id   = self.get_id(filters)
        url         = '/'.join([self.base_url + 'servers', str(server_id), 'stop'])

        http_code = ResumatorRightScale.make_request(self, url, "POST")

        if http_code == 201:
            self.logger.info("Terminated: %s" % server_name)
            return True

        self.logger.error("HTTP CODE: %d: Unable to terminate: %s" % (http_code, server_name))
        return False

    def delete(self, server_name):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': server_name}
        server_id   = self.get_id(filters)
        url         = '/'.join([self.base_url + 'servers', str(server_id)])

        http_code   = ResumatorRightScale.make_request(self, url, "DELETE")

        if http_code == 200:
            self.logger.info("Deleted Server: %s" % server_name)
            return True

        self.logger.error("HTTP CODE: %d: Unable to delete server: %s" % (http_code, server_name))
        return False


        