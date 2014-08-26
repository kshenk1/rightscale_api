
import pycurl, json, urllib, traceback, time, inspect, sys, logging, os
from cStringIO import StringIO

class ResumatorRightScale(object):
    
    def __init__(self, account_id, username, password, curl_debug=False):
        self.response_headers   = ''
        self.response_body      = ''
        self.account_id         = account_id
        self.username           = username
        self.password           = password
        #self.rightscale_url     = "https://my.rightscale.com"
        self.rightscale_url     = "https://us-4.rightscale.com"
        self.base_url           = "%s/api/acct/%d/" % (self.rightscale_url, self.account_id)
        self.url                = ""
        self.api_version        = "1.0"
        self.resp_format        = "js"
        self.curl_debug         = curl_debug
        self.errors             = []
        self.get_retries        = 3
        self.cookie_file        = os.path.expanduser(os.path.join("~", ".rightscale", "rs_api_cookie.txt"))

        self.__init_log('/var/log/rightscale_api', self.__class__.__name__)

    def __init_log(self, logpath, logname):       
        logger          = logging.getLogger(logname)

        if not len(logger.handlers):
            logfile     = '/'.join([logpath, "%s.log" % logname])
            format      = "%(levelname)s: %(asctime)s: %(message)s"
            dateformat  = "%Y/%m/%d %I:%M:%S %p"
            logger.setLevel(logging.DEBUG)
            fh          = logging.FileHandler(logfile)
            sh          = logging.StreamHandler()
            formatter   = logging.Formatter(format, dateformat)

            fh.setLevel(logging.DEBUG)
            sh.setLevel(logging.INFO)
            fh.setFormatter(formatter)
            sh.setFormatter(formatter)
            logger.addHandler(fh)
            logger.addHandler(sh)

        self.logger = logger

    def get_errors(self):
        return self.errors

    @staticmethod
    def get_credentials(creds_file):
        ## in case the check isn't made in the calling script...
        import sys, os
        if not os.path.exists(creds_file):
            print "%s does not exist!" % creds_file
            sys.exit(1)
        cred_options = {}
        for i, line in enumerate(file(creds_file)):
            line = line.rstrip("\r\n").lstrip()
            if not line or line.startswith(';') or line.startswith('#'):
                continue
            if '=' in line:
                name, value = line.split('=', 1)
                cred_options[name.rstrip().lower()] = value.replace('"', '').strip()

        return cred_options

    def get_details(self, filters={}, filter_operator='='):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        query_string    = self.url

        if filters:
            self.logger.debug("Filters sent: %s" % filters)
            query_string = self.url + "?filter=" + '&'.join("%s%s%s" % (key, filter_operator, urllib.quote(val)) for (key, val) in filters.iteritems())

        http_code   = self.make_request(query_string)

        if not self.response_body:
            return []

        contents    = json.loads(self.response_body)

        self.logger.debug("HTTP CODE: %d" % http_code)

        ## we get a 200 even if we have no data.
        if http_code == 200 and contents:
            self.logger.debug("Obtained %d results" % len(contents))
            return contents
        else:
            self.logger.error("Unable to obtain data!")
            return []

    def get_details_with_post_filter(self, filters):
        """
            So it appears as though there are only a few things that can be filtered while getting results.
            Most things (via rightscale) have to be all-or-nothing, and then filtered.
        """
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        
        http_code   = self.make_request(self.url)

        if not self.response_body:
            return []

        contents    = json.loads(self.response_body)

        self.logger.debug("Response Code: %d" % http_code)

        ## we get a 200 even if we have no data.
        if http_code == 200 and contents:
            self.logger.debug("Obtained %d results" % len(contents))

            if filters:
                self.logger.debug("Filters sent: %s" % filters)
                return_data = []
                for sa in contents:
                    matches = 0
                    for key, val in filters.iteritems():
                        if key in sa and sa[key] == val:
                            matches += 1
                    if matches == len(filters):
                        ## only return results where all filters have matched
                        return_data.append(sa)
                self.logger.debug("Sending back %d results" % len(return_data))
                return return_data
            else:
                return contents
        else:
            self.logger.error("HTTP CODE: %d: Unable to obtain data!" % http_code)
            return []

    def get_href(self, filters={}):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        content             = self.get_details(filters)
        num_results         = len(content)

        if num_results == 1:
            return content[0]['href']
        else:
            self.logger.error("Got %d results given filters: %s" % (num_results, filters))
            self.logger.error("results: %s" % content)
            return ""

    def get_id(self, filters={}):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        href    = self.get_href(filters)
        if not href:
            return 0
        h_array = href.split('/')
        _id     = h_array.pop()
        self.logger.debug("Got %s ID: %s" % (self.__class__.__name__, _id))
        return int(_id)

    def curl_debugger(debug_type, debug_msg):
        print "debug(%d): %s" % (debug_type, debug_msg)

    def pretty_print(self, data):
        if data:
            print json.dumps(data, indent=4, separators=(',', ':'))
        elif self.response_body:
            print json.dumps(self.response_body, indent=4, separators=(',', ':'))
        else:
            print "No data to print..."

    def get_audit_entry(self, url):
        url = url.strip() + "?format=%s" % self.resp_format

        http_code   = self.make_request(url)

        if http_code == 200:
            contents = json.loads(self.response_body)
            return contents
        
        return {}

    def get_audit_state(self, url):
        audit = self.get_audit_entry(url)

        if audit and 'state' in audit:
            return audit['state']
        return "unknown"

    def get_server_state(self, server_name):
        filters = {'nickname': server_name}
        details = self.get_details(filters)

        if details:
            try:
                return details[0]['state']
            except KeyError:
                pass
        return "unknown"

    def get_header_location(self):
        location = None
        if not self.response_headers:
            return None
        for key in self.response_headers:
            if key.startswith('Location'):
                location = key.split(':', 1)[1]
                break
        return location

    def __login(self):
        import subprocess
        curl_opts   = ['curl', '-s', '-S', '-c', self.cookie_file, '--user', '%s:%s' % (self.username, self.password), '--request', 'GET', "%slogin?api_version=%s" % (self.base_url, self.api_version)]

        self.logger.info("Logging in...")
        sub_call    = subprocess.Popen(curl_opts)
        sub_call.wait()
        if sub_call.returncode == 0:
            self.logger.info("Login Successful")
            return True

        self.logger.error("Login Failed!")
        return False


    def make_request(self, query_string, req_type="GET", post_data={}, retry=0):
        self.logger.debug("~~~~ %s ~~~~" % inspect.stack()[0][3])
        did_login   = False
        c           = pycurl.Curl()
        body        = StringIO()
        header      = StringIO()

        self.logger.debug("Query String: %s" % query_string)

        c.setopt(pycurl.URL,                query_string)
        c.setopt(pycurl.HTTPHEADER,         ["X-API-VERSION: %s" % self.api_version])
        c.setopt(pycurl.VERBOSE,            0)
        c.setopt(pycurl.FOLLOWLOCATION,     1)
        c.setopt(pycurl.USERPWD,            "%s:%s" % (self.username, self.password))
        c.setopt(pycurl.WRITEFUNCTION,      body.write)
        c.setopt(pycurl.HEADERFUNCTION,     header.write)

        c.setopt(pycurl.COOKIEFILE,         self.cookie_file)
        
        if self.curl_debug == True: c.setopt(pycurl.DEBUGFUNCTION,  self.curl_debugger)

        if req_type == "POST":
            c.setopt(pycurl.POST,           True)
            c.setopt(pycurl.POSTFIELDS,     urllib.urlencode(post_data))

        if req_type == "DELETE":
            c.setopt(pycurl.CUSTOMREQUEST,  "DELETE")

        if req_type == "PUT":
            c.setopt(pycurl.CUSTOMREQUEST,  "PUT")
            c.setopt(pycurl.POSTFIELDS,     urllib.urlencode(post_data))

        try:
            c.perform()
        except:
            traceback.print_exc(file=sys.stderr)

        self.response_headers   = header.getvalue().split('\r\n')
        self.response_body      = body.getvalue()

        ## because rightscale's API is unreliable, we're going to put a retry in here
        ## for any GET request. I've noticed that every once in a while rightscale's API
        ## doesn't return a response, so we don't have json to parse with blows things up.
        if req_type == "GET":
            try:
                json.loads(self.response_body, 'utf-8')
            except ValueError, e:
                ## HACK !!! - was just interested in getting this done...
                if (all(x in self.response_body.lower() for x in ['session', 'expired', 'invalid'])) or \
                (all(x in self.response_body.lower() for x in ['basic', 'auth', 'login'])):
                    self.response_body = ''
                    self.logger.warn("Session not set or expired!")
                    if self.__login():
                        did_login = True
                    else:
                        ## could handle this better, but not worth it at this time.
                        sys.exit(1)
                else:
                    self.logger.error("self.response_body is not a valid json object!")
                    self.logger.error("%s: %s" % (e, self.response_body))
                    self.response_body = ''
                    retry += 1
                    if retry < self.get_retries:
                        self.logger.warn("Trying the request again...")
                        time.sleep(0.5)
                        self.make_request(query_string, req_type, post_data, retry)
                    else:
                        self.logger.error("Hit the retry limit!")

        if did_login == True:
            return self.make_request(query_string, req_type, post_data, retry)
        else:
            ret = c.getinfo(pycurl.HTTP_CODE)
            c.close()
            return ret


