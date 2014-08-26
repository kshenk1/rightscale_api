##
## @Author:     Kyle Shenk <kyle.shenk@theresumator.com>
## @Created:    8/26/2014
##

import sys, os, threading
import json
from optparse import OptionParser

from ResumatorRightscale.ResumatorRightScale import ResumatorRightScale
from ResumatorRightscale.Server import Server
from ResumatorRightscale import Auditing

## TODO: Change this to use argparser http://docs.python.org/2.7/library/argparse.html

parser = OptionParser(
    version='%prog 1.0',
    epilog="""
        Server Deployment API with RightScale
    """)

parser.add_option('-b', '--git_branch',
    dest    = 'git_branch',
    help    = 'The git branch to checkout.')
parser.add_option('-n', '--no_cleanup',
    dest    = 'no_cleanup',
    action  = 'store_true',
    default = False,
    help    = 'If this flag is used, we will perform previous release cleanup on the server.')

parser.set_defaults(perform_cleanup=False)
(options, args) = parser.parse_args()

if not options.git_branch:
    options.git_branch = 'master'

creds_file      = os.path.expanduser(os.path.join("~", ".rightscale", "rs_api_creds.sh"))

if not os.path.exists(creds_file):
    print "ERROR: %s does not exist. This needs to contain authentication information to rightscale" % creds_file
    sys.exit(1)

def main():
    return_status       = 0
    thread_lock         = threading.Lock()
    polling_threads     = []

    creds   = ResumatorRightScale.get_credentials(creds_file)
    server  = Server(int(creds['rs_api_account_id']), creds['rs_api_user'], creds['rs_api_password'])

    api_git_repository  = 'git@github.com:resumator/numbers-api.git'
    server_name         = '(reporting) dashboard'
    rightscript_name    = 'Numbers API Git Checkout'

    data                = {
        'API_GIT_BRANCH':       "text:%s" % options.git_branch,
        'API_GIT_REPOSITORY':   "text:%s" % api_git_repository,
        'PERFORM_CLEANUP':      "text:%s" % ('true' if not options.no_cleanup else 'false'),
    }

    rs_version = 0
    
    rightscript_data = {
        'name':     rightscript_name,
        'version':  int(rs_version),
    }

    rightscript_threads = []

    if server.run_rightscript(server_name, rightscript_data, data):
        location = server.get_header_location()

        if location:
            script_thread = Auditing.AuditEntry(thread_lock, server, server_name, rightscript_data['name'], location, 5)
            script_thread.start()
            rightscript_threads.append(script_thread)

        if rightscript_threads:
            for t in rightscript_threads:
                t.join()

        if not script_thread or script_thread.entry_exit_state == 'failed':
            server.logger.error("Deployment to %s failed!" % server_name)
            return_status = 1
        else:
            server.logger.info("Deployment to %s complete!" % server_name)
    else:
        server.logger.error("%s failed on %s" % (rightscript_data['name'], server_name))

    return return_status


return_status = 0
if __name__ == "__main__":
    return_status = main()

sys.exit(return_status)

