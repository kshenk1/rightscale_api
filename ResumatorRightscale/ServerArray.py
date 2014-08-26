
from ResumatorRightScale import ResumatorRightScale
from ServerTemplate import ServerTemplate
from SecurityGroup import SecurityGroup
from SshKey import SshKey
from RightScript import RightScript
import urllib, sys, json, inspect, subprocess, itertools

class ServerArray(ResumatorRightScale):

    def __init__(self, account_id, username, password):
        ResumatorRightScale.__init__(self, account_id, username, password)

        self.url        = self.base_url + "server_arrays.%s" % self.resp_format

    def get_details(self, filters={}, debug=False):
        return self.get_details_with_post_filter(filters)

    def get_instances(self, filters={}):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id), "instances?format=%s" % self.resp_format])

        http_code   = ResumatorRightScale.make_request(self, url)
        contents    = json.loads(self.response_body)
        
        if http_code == 200 and contents:
            return contents
        elif http_code == 200 and not contents:
            self.logger.warn("No instances returned for %s" % filters['nickname'])
            return []

        self.logger.error("HTTP CODE: %d: Unable to get instances." % http_code)
        return []

    def add_security_groups(self, array_name, security_groups):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        array_id    = self.get_id({'nickname': array_name})
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id)])

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
            groups.append("server_array[ec2_security_groups_href][]=%s" % sg.get_href({'aws_group_name': s_group}))

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

    def run_recipe(self, array_name, recipe, inputs={}):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': array_name}
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id), 'run_executable'])

        post_data   = {'recipe': recipe}
        post_data   = dict(map(lambda (key, value): ("server[" + str(key) + "]", value), post_data.items()))

        if inputs:
            inputs      = dict(map(lambda (key, value): ("server[parameters][" + str(key) + "]", value), inputs.items()))
            post_data   = dict(post_data.items() + inputs.items())

        self.logger.debug("post data: %s" % post_data)
        self.logger.info("Running recipe: %s on %s" % (recipe, array_name))

        http_code   = ResumatorRightScale.make_request(self, url, "POST", post_data)
        
        if http_code == 201:
            self.logger.info("Successfully executed %s on %s" % (recipe, array_name))

            return True

        self.logger.error("HTTP CODE: %d: %s failed on %s" % (http_code, recipe, array_name))
        return False

    def run_rightscript(self, array_name, script_data, inputs={}, all_servers=True):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': array_name}
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id), ('run_script_on_all' if all_servers else 'run_script') + "?format=%s" % self.resp_format])

        rs          = RightScript(self.account_id, self.username, self.password)

        ## we could also do this with the ServerTemplate module and pass a name, but
        ## in this case I think it makes more sense to pass in the href because we want to send
        ## the server template that is being used by the array.
        server_template_href = script_data.pop('server_template_href', None)

        post_data   = {
            'right_script_href':        rs.get_href(script_data),
            'server_template_hrefs':    server_template_href
        }

        post_data   = dict(map(lambda (key, value): ("server_array[" + str(key) + "]", value), post_data.items()))

        if inputs:
            inputs      = dict(map(lambda (key, value): ("server_array[parameters][" + str(key) + "]", value), inputs.items()))
            post_data   = dict(post_data.items() + inputs.items())

        self.logger.debug("post data: %s" % post_data)
        self.logger.info("Running rightscript: \"%s\" on %s" % (', '.join(["%s: %s" % (key, val) for (key, val) in script_data.items()]), array_name))

        http_code   = ResumatorRightScale.make_request(self, url, "POST", post_data)
        
        if http_code == 201:
            self.logger.info("Successfully executed %s on %s" % (script_data['name'], array_name))

            return True

        self.logger.error("HTTP CODE: %d: %s failed on %s" % (http_code, script_data['name'], array_name))
        return False

    def update_input(self, array_name, params):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        if len(params) > 0:
            
            self.logger.info("~" * 50)
            self.logger.info("Updating parameters for array: %s" % array_name)
            self.logger.info("    ~~~~ PARAMETERS ~~~~")
            for k, v in params.items():
                self.logger.info("%s: %s" % (k, v))
            self.logger.info("~" * 50)

            filters     = {'nickname': array_name}
            array_id    = self.get_id(filters)
            update_data = dict(map(lambda (key, value): ("server[parameters][" + str(key) + "]", value), params.items()))

            url = '/'.join([self.base_url + 'servers', str(array_id)])

            http_code = ResumatorRightScale.make_request(self, url, "PUT", update_data)

            if http_code == 204:
                self.logger.info("%s: Updated inputs" % array_name)
                return True
            
            self.logger.error("HTTP CODE: %d: %s: Unable to update inputs!" % (http_code, array_name))
            return False

        self.logger.error("No params sent")
        return False

    def create(self, server_data, parameters={}, cloud_id=1):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])

        array_name      = (server_data['nickname'] if 'nickname' in server_data else 'unknown')

        st              = ServerTemplate(self.account_id, self.username, self.password)
        ssh             = SshKey(self.account_id, self.username, self.password)

        elasticity_data = server_data.pop('elasticity', None)
        security_groups = server_data.pop('security_groups', None)
        ssh_key_data    = server_data.pop('ssh_key', None)
        template_data   = server_data.pop('server_template', None)

        array_enabled   = ('Yes' if 'active' in server_data and (server_data['active'] == 'true' or server_data['active'] == 'true') else 'No')

        self.logger.info("~" * 50)
        self.logger.info("Creating Server Array: %s" % array_name)
        self.logger.info("              Enabled: %s" % array_enabled)
        self.logger.info("      Server Template: %s" % ', '.join(["%s: %s" % (key, val) for (key, val) in template_data.items()]))
        self.logger.info("           Array Type: %s" % server_data['array_type'])
        self.logger.info("      Security Groups: %s" % ', '.join(security_groups))
        self.logger.info("        Instance Type: %s" % server_data['instance_type'])
        #self.logger.info("              Pricing: %s" % server_data['pricing'])
        self.logger.info("              SSH Key: %s" % ssh_key_data['aws_key_name'])
        self.logger.info("             Cloud Id: %d" % cloud_id)
        self.logger.info("           Elasticity: %s" % ', '.join(["%s: %s" % (key, val) for (key, val) in elasticity_data.items()]))
        if parameters:
            self.logger.info("    ~~~~ PARAMETERS ~~~~")
            for k, v in parameters.items():
                self.logger.info("%s: %s" % (k, v))
        self.logger.info("~" * 50)

        server_data['server_template_href'] = st.get_href(template_data)
        server_data['ec2_ssh_key_href']     = ssh.get_href(ssh_key_data)

        server_data     = dict(map(lambda (key, value): ("server_array[" + str(key) + "]", value), server_data.items()))

        elasticity_data = dict(map(lambda (key, value): ("server_array[elasticity][" + str(key) + "]", value), elasticity_data.items()))
        server_data     = dict(server_data.items() + elasticity_data.items())

        if parameters:
            param_data  = dict(map(lambda (key, value): ("server_array[parameters][" + str(key) + "]", value), parameters.items()))
            server_data = dict(server_data.items() + param_data.items())

        if cloud_id != 1:
            self.logger.warn("Deviating from default cloud id of 1!")

        server_data['cloud_id'] = cloud_id

        self.logger.debug("Server array post data: %s" % server_data)

        http_code = ResumatorRightScale.make_request(self, self.url, "POST", server_data)

        if http_code == 201:
            self.logger.info("Created server array '%s'" % array_name)
            return self.add_security_groups(array_name, security_groups)
        
        self.logger.error("HTTP CODE: %d: Unable to create '%s'" % (http_code, array_name))
        return False

    def launch(self, array_name):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': array_name}
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id), 'launch'])

        self.logger.info("Launching Array: %s" % array_name)

        http_code   = ResumatorRightScale.make_request(self, url, "POST")

        if http_code == 201:
            self.logger.info("%s launched" % array_name)
            return True

        self.logger.error("HTTP CODE: %d: Server Array launch failed for: %s" % (http_code, array_name))
        return False

    def terminate(self, array_name):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': array_name}
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id), 'terminate_all'])

        http_code = ResumatorRightScale.make_request(self, url, "POST")

        if http_code == 201:
            self.logger.info("Terminated: %s" % array_name)
            return True

        self.logger.error("HTTP CODE: %d: Unable to terminate: %s" % (http_code, array_name))
        return False

    def delete(self, array_name):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': array_name}
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id)])

        http_code   = ResumatorRightScale.make_request(self, url, "DELETE")

        if http_code == 200:
            self.logger.info("Deleted Server Array: %s" % array_name)
            return True

        self.logger.error("HTTP CODE: %d: Unable to delete server array: %s" % (http_code, array_name))
        return False

    def enable(self, array_name):
        return self.__array_activation(array_name, True)

    def disable(self, array_name):
        return self.__array_activation(array_name, False)

    def __array_activation(self, array_name, activate=True):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        filters     = {'nickname': array_name}
        array_id    = self.get_id(filters)
        url         = '/'.join([self.base_url + 'server_arrays', str(array_id)])
        _act        = ('true' if activate else 'false')
        _verb       = ('enable' if activate else 'disable')
        _past       = ('Enabled' if activate else 'Disabled')

        post_data   = {'server_array[active]': _act}

        http_code   = ResumatorRightScale.make_request(self, url, "PUT", post_data)

        if http_code == 204:
            self.logger.info("%s Server Array: %s" % (_past, array_name))
            return True

        self.logger.error("Unable to %s %s" % (_verb, array_name))
        return False



