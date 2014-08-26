
import threading, time
from ResumatorRightscale import Auditing
from ResumatorRightscale.Server import Server
from ResumatorRightscale.EbsSnapshot import EbsSnapshot
from ResumatorRightscale.EbsVolume import EbsVolume

class DbRefreshHelper(threading.Thread):
    def __init__(self, creds, master, slaves, backup_lineage, thread_lock=None, do_force_reset=False, do_set_dns_slave=True, refresh_lineage=None, timestamp_override=None, progress_output=True):
        threading.Thread.__init__(self)
        self.entry_exit_state   = None
        
        self.server             = Server(int(creds['rs_api_account_id']), creds['rs_api_user'], creds['rs_api_password'])
        self.ebss               = EbsSnapshot(int(creds['rs_api_account_id']), creds['rs_api_user'], creds['rs_api_password'])
        self.ebsv               = EbsVolume(int(creds['rs_api_account_id']), creds['rs_api_user'], creds['rs_api_password'])

        self.master             = master
        self.slaves             = slaves
        self.do_force_reset     = do_force_reset
        self.do_set_dns_slave   = do_set_dns_slave
        self.backup_lineage     = backup_lineage
        self.refresh_lineage    = refresh_lineage
        self.timestamp_override = timestamp_override
        self.progress_output    = progress_output

        if not thread_lock:
            self.thread_lock    = threading.Lock()
        else:
            self.thread_lock    = thread_lock

        self.blocking           = False

    def run(self):
        self.server.logger.info("Starting to refresh the databases...")
        # Get lock to synchronize threads
        self.thread_lock.acquire(self.blocking)

        if self.do_force_reset == True:
            f_servers = []
            f_servers.append(self.master)
            if self.slaves:
                for s in self.slaves:
                    f_servers.append(s)
            failed_servers = self.force_reset(f_servers)
            if failed_servers:
                self.server.logger.error("Failed to force reset servers. Quitting as the refresh will not work properly")
                self.entry_exit_state = 'failed'
                return
            ## give it a moment....
            self.server.logger.info("Sleeping for a moment...")
            time.sleep(15)

        mserver                 = self.server.get_details({'nickname': self.master})
        master_instance_href    = mserver[0]['current_instance_href']

        if not self.restore_master():
            self.entry_exit_state = 'failed'
            return

        ## if there are no slaves, then we're done here.
        if not self.slaves:
            self.entry_exit_state = 'successful'
            return

        if not self.backup_master():
            self.entry_exit_state = 'failed'
            return

        polling_threads = []

        volumes         = self.ebsv.get_details({'ec2_instance_href': master_instance_href})
        ebs_nickname    = self.ebsv.get_nickname(volumes)
        thread_lock     = threading.Lock()
        db_thread       = Auditing.AuditSnapshot(thread_lock, self.ebss, ebs_nickname, progress_output=self.progress_output)
        db_thread.start()
        polling_threads.append(db_thread)

        if polling_threads:
            map(lambda t: t.join(), polling_threads)

        latest_timestamp = self.ebss.get_latest_timestamp(self.server, self.master)

        self.ebss.logger.info("Latest Snapshot Timestamp on %s: %s" % (self.master, latest_timestamp))
        time.sleep(5)

        if not self.restore_slaves(latest_timestamp):
            self.entry_exit_state = 'failed'
            return

        # Free lock to release next thread
        if self.thread_lock.locked():
            self.thread_lock.release()

        self.entry_exit_state = 'successful'
        return

    def restore_master(self):
        polling_threads = []
        thread_lock     = threading.Lock()
        db_thread       = None
        recipe          = 'db::do_primary_restore_and_set_master'
        if self.timestamp_override and self.refresh_lineage:
            self.server.logger.info("Restoring %s using refresh lineage %s and timestamp override: %s" % 
                (self.master, self.refresh_lineage, self.timestamp_override))
            recipe_data     = {
                'db/backup/lineage':            "text:%s" % self.refresh_lineage,
                'db/backup/timestamp_override': "text:%s" % self.timestamp_override
            }
        else:
            if self.refresh_lineage:
                self.server.logger.info("Restoring %s using backup lineage: %s" % (self.master, self.refresh_lineage))
                recipe_data     = {
                    'db/backup/lineage':            "text:%s" % self.refresh_lineage,
                }
            else:
                self.server.logger.info("Restoring %s using timestamp override: %s" % (self.master, self.timestamp_override))
                recipe_data     = {
                    'db/backup/lineage':            "text:%s" % self.refresh_lineage,
                    'db/backup/timestamp_override': "text:%s" % self.timestamp_override
                }
        
        if self.server.run_recipe(self.master, recipe, recipe_data):
            location = self.server.get_header_location()

            if location:
                db_thread = Auditing.AuditEntry(thread_lock, self.server, self.master, recipe, location, 20, progress_output=self.progress_output)
                db_thread.start()
                polling_threads.append(db_thread)
        else:
            self.server.logger.error("Unable to restore %s!" % self.master)
            return False

        if polling_threads:
            map(lambda t: t.join(), polling_threads)

        if not db_thread or db_thread.entry_exit_state == 'failed':
            return False

        self.server.logger.info("Sleeping for a couple seconds to allow the volumes to settle...")
        time.sleep(20)
        return True

    def backup_master(self):
        polling_threads = []
        thread_lock     = threading.Lock()
        retries         = 2
        current_try     = 0
        recipe          = 'db::do_primary_backup'
        recipe_data     = {
            'db/backup/lineage': "text:%s" % self.backup_lineage
        }
        final_status    = None
        location        = None
        db_thread       = None

        while (current_try <= retries):
            current_try = current_try + 1
            if self.server.run_recipe(self.master, recipe, recipe_data):
                location        = self.server.get_header_location()

                if location:
                    db_thread = Auditing.AuditEntry(thread_lock, self.server, self.master, recipe, location, progress_output=self.progress_output)
                    db_thread.start()
                    polling_threads.append(db_thread)
            else:
                ## if we fail to run the recipe, we're done.
                self.server.logger.error("Failed to run %s on %s, quitting!" % (recipe, self.master))
                return False

            if polling_threads:
                map(lambda t: t.join(), polling_threads)

            if db_thread and db_thread.entry_exit_state:
                final_status = db_thread.entry_exit_state

            if final_status == 'completed':
                break

            if current_try <= retries:
                self.server.logger.warn("Trying again in 5 seconds...")
                time.sleep(5)
            
        if not final_status or final_status == 'failed':
            return False

        self.server.logger.info("Sleeping for a couple seconds to allow the volumes to settle...")
        time.sleep(20)
        return True

    def restore_slaves(self, master_timestamp):
        polling_threads = []
        db_threads      = []
        thread_lock     = threading.Lock()
        db_thread       = None
        recipe          = 'db::do_primary_init_slave'
        recipe_data     = {
            'db/backup/lineage':            "text:%s" % self.backup_lineage,
            'db/backup/timestamp_override': "text:%s" % master_timestamp
        }
        recipes_after   = ['db::request_master_allow']

        if self.do_set_dns_slave:
            recipes_after.append('db::do_set_dns_slave')

        failed_servers          = []
        failed_servers_recipes  = []

        ## let's do the init slave first....
        for slave in self.slaves:
            location = None
            if self.server.run_recipe(slave, recipe, recipe_data):
                location = self.server.get_header_location()

                if location:
                    db_thread = Auditing.AuditEntry(thread_lock, self.server, slave, recipe, location, 20, progress_output=self.progress_output)
                    db_thread.start()
                    _thread_data = {slave: db_thread}
                    db_threads.append(_thread_data)
            else:
                failed_servers.append(slave)

        if db_threads:
            for d in db_threads:
                for slave, thread in d.iteritems():
                    thread.join()
            for d in db_threads:
                for slave, thread in d.iteritems():
                    if thread.entry_exit_state == 'failed':
                        self.server.logger.error("%s failed for %s" % (recipe, slave))
                        ## should not be in there already, but just making sure...
                        if not slave in failed_servers:
                            failed_servers.append(slave)

        ## and now we'll do the rest of the recipe's
        for slave in self.slaves:
            if not slave in failed_servers:
                for r in recipes_after:
                    location = None
                    if self.server.run_recipe(slave, r):
                        location = self.server.get_header_location()

                        if location:
                            db_thread = Auditing.AuditEntry(thread_lock, self.server, slave, r, location, progress_output=self.progress_output)
                            db_thread.start()
                            polling_threads.append(db_thread)
                    else:
                        _fail_data = {slave: {'recipe': r}}
                        failed_servers_recipes.append(_fail_data)

        if polling_threads:
            map(lambda t: t.join(), polling_threads)
        
        ## if those recipes failed, then log a warning
        if failed_servers_recipes:
            for f in failed_servers_recipes:
                for _server, data in f.iteritems():
                    self.server.logger.warn("Recipe \"%s\" failed on server \"%s\"" % (data['recipe'], _server))
        
        ## only return false if we were unable to refresh a slave. This does constitute a failure.
        if failed_servers:
            self.server.logger.error("We failed to refresh: %s" % ', '.join(failed_servers))
            return False

        self.server.logger.info("Slaves data has been restored from the master.")
        return True

    def force_reset(self, reset_servers, retry=1):
        thread_lock         = threading.Lock()
        retry_limit         = 2
        polling_threads     = []
        successful_servers  = []
        failed_servers      = []

        recipe  = 'db::do_force_reset'
        data    = {'db/force_safety': 'text:off'}

        self.server.logger.info("Force Resetting: %s" % ', '.join(reset_servers))

        for s in reset_servers:
            if self.server.run_recipe(s, recipe, data):
                location = self.server.get_header_location()

                if location:
                    db_thread = Auditing.AuditEntry(thread_lock, self.server, s, recipe, location, 20, progress_output=self.progress_output)
                    db_thread.start()
                    thread_data = {
                        'nickname': s,
                        'thread':   db_thread
                    }
                    polling_threads.append(thread_data)
            else:
                failed_servers.append(s)

        if polling_threads:
            for t in polling_threads:
                t['thread'].join()
                if t['thread'].entry_exit_state == 'failed':
                    if t['nickname'] not in failed_servers:
                        failed_servers.append(t['nickname'])
                else:
                    if t['nickname'] not in successful_servers:
                        successful_servers.append(t['nickname'])

        ## fixes the false failure when recursion happens because failed_servers persists
        if successful_servers:
            for s in successful_servers:
                if s in failed_servers:
                    failed_servers.remove(s)

        if len(failed_servers) > 0 and retry <= retry_limit:
            retry += 1
            self.server.logger.warn("Server(s): %s - failed to force reset, trying again..." % ', '.join(failed_servers))
            time.sleep(5)
            self.force_reset(failed_servers, retry)

        if successful_servers:
            self.server.logger.info("Successfully Forced Reset: %s" % ', '.join(successful_servers))

        if failed_servers:
            self.server.logger.warn("Failed to force reset: %s" % ', '.join(failed_servers))
            if retry > retry_limit:
                self.server.logger.warn("We've exceeded the retry limit for force resetting these servers!")

        return failed_servers


