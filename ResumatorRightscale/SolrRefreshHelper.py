
import threading, time
from ResumatorRightscale import Auditing
from ResumatorRightscale.Server import Server

class SolrRefreshHelper(threading.Thread):
    def __init__(self, creds, master, slave=None, thread_lock=None, do_force_reset=False, refresh_lineage=None, progress_output=True):
        threading.Thread.__init__(self)
        self.entry_exit_state   = None
        
        self.server             = Server(int(creds['rs_api_account_id']), creds['rs_api_user'], creds['rs_api_password'])

        self.master             = master
        self.slave              = slave
        self.do_force_reset     = do_force_reset
        self.refresh_lineage    = refresh_lineage
        self.progress_output    = progress_output

        if not thread_lock:
            self.thread_lock    = threading.Lock()
        else:
            self.thread_lock    = thread_lock

        self.blocking           = False

    def run(self):
        self.server.logger.info("Starting to refresh Solr with backup lineage override: %s" % self.refresh_lineage)
        self.thread_lock.acquire(self.blocking)

        if self.do_force_reset == True:
            _failed_servers = self.solr_shutdown()

            if _failed_servers:
                self.server.logger.error("Unable to shutdown solr servers!")
                self.entry_exit_state = 'failed'
                return

            self.solr_startup()

        if not self.restore_master():
            self.entry_exit_state = 'failed'
            return

        self.entry_exit_state = 'successful'

        ## if there are no slaves, then we're done here.
        if not self.slave:
            return

        ## sleep for a couple seconds before creating the volumes on the slave(s)
        time.sleep(5)
        if not self.restore_slaves():
            self.entry_exit_state = 'failed'
            return

        if self.thread_lock.locked():
            self.thread_lock.release()

        return

    def restore_master(self):
        polling_threads = []
        db_thread       = None
        recipe          = 'solr::do_storage_restore'
        recipe_data     = {
            'solr/backup_lineage_override': "text:%s" % self.refresh_lineage
        }

        if self.server.run_recipe(self.master, recipe, recipe_data):
            location = self.server.get_header_location()

            if location:
                db_thread = Auditing.AuditEntry(self.thread_lock, self.server, self.master, recipe, location, progress_output=self.progress_output)
                db_thread.start()
                polling_threads.append(db_thread)

        if polling_threads:
            map(lambda t: t.join(), polling_threads)

        if db_thread and db_thread.entry_exit_state == 'failed':
            return False

        return True

    def restore_slaves(self):
        polling_threads = []
        db_thread       = None
        recipe          = 'solr::do_storage_create'

        if self.server.run_recipe(self.slave, recipe):
            location = self.server.get_header_location()

            if location:
                db_thread = Auditing.AuditEntry(self.thread_lock, self.server, self.slave, recipe, location, progress_output=self.progress_output, time_limit_minutes=15)
                db_thread.start()
                polling_threads.append(db_thread)

        if polling_threads:
            map(lambda t: t.join(), polling_threads)

        if db_thread and db_thread.entry_exit_state == 'failed':
            return False

        return True

    def solr_shutdown(self):
        polling_threads = []
        block_failed    = []
        failed_servers  = []

        recipe = 'block_device::do_delete_volumes_and_terminate_server'
        recipe_data = {
            'block_device/terminate_safety':    "text:off"
        }

        _servers = [self.master, self.slave]

        for s in _servers:
            if self.server.run_recipe(s, recipe, recipe_data):
                location = self.server.get_header_location()

                if location:
                    db_thread = Auditing.AuditEntry(self.thread_lock, self.server, s, recipe, location, progress_output=self.progress_output, time_limit_minutes=10)
                    db_thread.start()
                    _thread_data = {
                        'nickname': s,
                        'thread':   db_thread
                    }
                    polling_threads.append(_thread_data)

        if polling_threads:
            for t in polling_threads:
                t['thread'].join()

                if t['thread'].entry_exit_state and t['thread'].entry_exit_state != 'completed':
                    block_failed.append(t['nickname'])

        polling_threads = []

        if block_failed:
            for s in block_failed:
                if self.server.terminate(s):
                    db_thread = Auditing.AuditServer(self.thread_lock, self.server, s)
                    db_thread.start()
                    _thread_data = {
                        'nickname': s,
                        'thread':   db_thread
                    }
                    polling_threads.append(_thread_data)

        if polling_threads:
            for t in polling_threads:
                t['thread'].join()
                if t['thread'].entry_exit_state and (t['thread'].entry_exit_state not in ('stopped', 'terminated')):
                    failed_servers.append(t['nickname'])

        return failed_servers

    def solr_startup(self):
        polling_threads = []
        failed_servers  = []

        _servers = [self.master, self.slave]

        for s in _servers:
            if self.server.launch(s):
                s_thread    = Auditing.AuditServer(self.thread_lock, self.server, s)
                s_thread.start()
                thread_data = {
                    'nickname': s,
                    'thread':   s_thread
                }
                polling_threads.append(thread_data)

        if polling_threads:
            for t in polling_threads:
                t['thread'].join()
                if t['thread'].entry_exit_state and t['thread'].entry_exit_state != 'completed':
                    failed_servers.append(t['nickname'])

        return failed_servers

