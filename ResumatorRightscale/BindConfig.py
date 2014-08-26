import os, subprocess
from ResumatorRightscale.LogWrapper import LogWrapper

class BindConfig():
    def __init__(self):
        base_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/..')
        
        self.template_dir           = '/'.join([base_path, 'data', 'bind_templates'])
        self.config_out_dir         = '/'.join([base_path, 'data', 'bind_configs_out'])

        self.zone_destination_dir   = '/var/named/dynamic'
        self.conf_destination_dir   = '/etc/named'

        self.zone_template_filename = 'zone.template'
        self.conf_template_filename = 'conf.template'

        self.tokens         = {
            'domain_name':          '{{DOMAIN_NAME}}',
            'serial':               '{{SERIAL}}',
            'lb_ip':                '{{LB_IP}}',
            'bind_ip':              '{{BIND_IP}}',
            'zone_destination_dir': '{{ZONE_DESTINATION_DIR}}'
        }

        logger                  = LogWrapper('/var/log/rightscale_api', self.__class__.__name__)
        self.logger             = logger.get_logger()

    def __get_zone_template(self):
        return '/'.join([self.template_dir, self.zone_template_filename])

    def __get_conf_template(self):
        return '/'.join([self.template_dir, self.conf_template_filename])

    def __parse_file(self, replacements, domain_name, template_file, file_type):
        create_file = '/'.join([self.config_out_dir, "%s.%s" % (domain_name, file_type)])

        if file_type == 'conf':
            replacements['zone_destination_dir'] = self.zone_destination_dir

        with open(template_file, mode='r', buffering=-1) as in_f, open(create_file, mode='w', buffering=-1) as out_f:
            for line in in_f:
                for key, val in replacements.iteritems():
                    if key in self.tokens:
                        line = line.replace(self.tokens[key], str(val))
                out_f.write(line)
        if os.path.isfile(create_file):
            self.logger.info("Created: %s" % create_file)
            return create_file
        self.logger.error("Unable to create: %s" % create_file)
        return None

    def __manage_named_conf(self, host, domain_name, action, use_sudo=True):
        args = ['ssh', host, '/usr/local/sbin/dnsupdate-conf.sh', action, domain_name]

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call    = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stderr      = sub_call.stderr.readline().strip()
        stdout      = sub_call.stdout.readline().strip()
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error(stderr)
            return False
        self.logger.info(stdout)
        return True

    def parse_zone(self, replacements, domain_name):
        return self.__parse_file(replacements, domain_name, self.__get_zone_template(), 'zone')

    def parse_conf(self, replacements, domain_name):
        return self.__parse_file(replacements, domain_name, self.__get_conf_template(), 'conf')

    def add_domain_conf(self, host, domain_name, use_sudo=True):
        return self.__manage_named_conf(host, domain_name, 'add', use_sudo)
        
    def remove_domain_conf(self, host, domain_name, use_sudo=True):
        return self.__manage_named_conf(host, domain_name, 'remove', use_sudo)

    def add_subdomain(self, host, domain_name, subdomain, ip_address, ttl=60, use_sudo=True):
        args    = ['ssh', host, '/usr/local/sbin/dnsupdate-add.sh', domain_name, subdomain, str(ttl), ip_address]
        devnull = open(os.devnull, 'w')

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call    = subprocess.Popen(args, stdout=devnull, stderr=subprocess.PIPE)
        stderr      = sub_call.stderr.readline().strip()
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error(stderr)
            return False
        self.logger.info("Subdomain %s added to %s" % (subdomain, domain_name))
        return True

    def restart_named(self, host, use_sudo=True):
        args    = ['ssh', host, 'service', 'named', 'restart']
        devnull = open(os.devnull, 'w')

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call    = subprocess.Popen(args, stdout=devnull, stderr=subprocess.PIPE)
        stderr      = sub_call.stderr.readline().strip()
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error(stderr)
            return False
        self.logger.info("Successfully restarted named")
        return True

    def rndc_reload(self, host, use_sudo=True):
        args    = ['ssh', host, 'rndc', 'reload']
        devnull = open(os.devnull, 'w')

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call    = subprocess.Popen(args, stdout=devnull, stderr=subprocess.PIPE)
        stderr      = sub_call.stderr.readline().strip()
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error(stderr)
            return False
        self.logger.info("Successfully ran rndc reload")
        return True

    def send_file(self, source_file, host, destination, use_sudo=True):
        if not os.path.isfile(source_file):
            self.logger.error("%s doesn't exist!" % source_file)
            return False

        args    = ['scp', source_file, "%s:%s" % (host, destination)]
        devnull = open(os.devnull, 'w')

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call    = subprocess.Popen(args, stdout=devnull, stderr=subprocess.PIPE)
        stderr      = sub_call.stderr.readline().strip()
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error("Unable to send file: %s to %s" % (source_file, destination))
            self.logger.error("STDERR: %s" % stderr)
            return False
        self.logger.info("File: %s sent to %s:%s" % (source_file, host, destination))
        return True

    def chown_remote_file(self, host, destination_file, user, group, use_sudo=True):
        args    = ['ssh', host, 'chown', "%s:%s" % (user, group), destination_file]
        devnull = open(os.devnull, 'w')

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call    = subprocess.Popen(args, stdout=devnull, stderr=subprocess.PIPE)
        stderr      = sub_call.stderr.readline().strip()
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error(stderr)
            return False
        self.logger.info("Successfully changed ownership of %s to %s:%s" % (destination_file, user, group))

        return True

    def remove_remote_file(self, host, destination_file, use_sudo=True):
        args    = ['ssh', host, 'rm', '-f', destination_file]
        devnull = open(os.devnull, 'w')

        if use_sudo: args.insert(0, 'sudo')

        self.logger.debug("CMD args: %s" % args)

        sub_call = subprocess.Popen(args, stdout=devnull, stderr=devnull)
        sub_call.wait()

        if sub_call.returncode != 0:
            self.logger.error("Unable to remove remote file: %s from %s" % (destination_file, host))
            return False
        self.logger.info("Removed remote file: %s:%s" % (host, destination_file))
        return True 
