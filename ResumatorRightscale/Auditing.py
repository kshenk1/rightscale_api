
import threading, time

class AuditEntry(threading.Thread):
    def __init__(self, thread_lock, server_obj, server_name, recipe, url, sleep_time=10, progress_output=True, time_limit_minutes=0):
        threading.Thread.__init__(self)
        self.entry_exit_state   = None
        self.server             = server_obj
        self.server_name        = server_name
        self.recipe             = recipe
        self.url                = url
        self.thread_lock        = thread_lock
        self.blocking           = False
        self.sleep_time         = sleep_time
        self.progress_output    = progress_output
        self.time_limit_minutes = int(time_limit_minutes)
        self.expiration_time    = None
        self.start_time         = time.time()
    def run(self):
        self.server.logger.info("Starting Poll for recipe/script \"%s\" on %s" % (self.recipe, self.server_name))
        if self.progress_output == False:
            self.server.logger.info("Progress output is turned off. This operation will be silent until finished.")

        if self.time_limit_minutes > 0:
            self.expiration_time = self.start_time + (self.time_limit_minutes*60)
            self.server.logger.info("Time limit set to %d minutes." % self.time_limit_minutes)
        # Get lock to synchronize threads
        self.thread_lock.acquire(self.blocking)
        self.get_audit_state()
        # Free lock to release next thread
        if self.thread_lock.locked():
            self.thread_lock.release()

    def get_audit_state(self):
        exit_states = ('completed', 'failed')
        while True:

            state = self.server.get_audit_state(self.url)
            if state in exit_states:
                self.entry_exit_state = state
                msg = "%s %s for %s" % (self.recipe, state, self.server_name)
                if state == 'failed':
                    self.server.logger.error(msg)
                else:
                    self.server.logger.info(msg)
                break

            if self.time_limit_minutes > 0:
                if time.time() > self.expiration_time:
                    self.server.logger.warn("%s, %s: Exceeded the time limit of %d minutes!" % (self.server_name, self.recipe, self.time_limit_minutes))
                    self.entry_exit_state = 'failed'
                    break

            if self.progress_output:
                self.server.logger.info("%s still running on %s, current state: %s" % (self.recipe, self.server_name, state))
            time.sleep(self.sleep_time)

class AuditServer(threading.Thread):
    def __init__(self, thread_lock, server_obj, server_name, sleep_time=20, progress_output=True):
        threading.Thread.__init__(self)
        self.entry_exit_state   = None
        self.server             = server_obj
        self.server_name        = server_name
        self.thread_lock        = thread_lock
        self.blocking           = False
        self.sleep_time         = sleep_time
        self.progress_output    = progress_output
    def run(self):
        ## give this a couple seconds before we actually start polling...
        ## I launched a queue server once and it stopped polling immediately because the initial state of the server that
        ## rightscale returned was 'stopped'. So, this sleep is here in attempts to keep that from happening. This happened
        ## once out of hundreds of launches, but apparently it's possible...
        time.sleep(2)
        self.server.logger.info("Starting Completion Status Poll for %s" % self.server_name)
        if self.progress_output == False:
            self.server.logger.info("Progress output is turned off. This operation will be silent until finished.")
        # Get lock to synchronize threads
        self.thread_lock.acquire(self.blocking)
        self.get_server_state()
        # Free lock to release next thread
        if self.thread_lock.locked():
            self.thread_lock.release()

    def get_server_state(self):
        exit_states = ('operational', 'stopped', 'terminated', 'stranded', 'stranded in booting')
        while True:
            state = self.server.get_server_state(self.server_name)
            if state in exit_states:
                self.entry_exit_state = state
                self.server.logger.info("%s is %s" % (self.server_name, state))
                break
            if self.progress_output:
                self.server.logger.info("%s still building, current state: %s" % (self.server_name, state))
            time.sleep(self.sleep_time)

class AuditInstances(threading.Thread):
    def __init__(self, thread_lock, array_obj, array_name, target_state='operational', sleep_time=20, progress_output=True):
        threading.Thread.__init__(self)
        self.entry_exit_state   = None
        self.array              = array_obj
        self.array_name         = array_name
        self.thread_lock        = thread_lock
        self.blocking           = False
        self.sleep_time         = sleep_time
        self.target_state       = target_state
        self.progress_output    = progress_output
        self.instances_failed   = []
    def run(self):
        self.array.logger.info("Starting Completion Status Poll for %s, target state: %s" % (self.array_name, self.target_state))
        if self.progress_output == False:
            self.array.logger.info("Progress output is turned off. This operation will be silent until finished.")
        # Get lock to synchronize threads
        self.thread_lock.acquire(self.blocking)
        self.get_array_state()
        # Free lock to release next thread
        if self.thread_lock.locked():
            self.thread_lock.release()

    def get_array_state(self):
        arrays              = ['seed']
        ## not used, but might return to this method of exiting...
        exit_states         = ('operational', 'stopped', 'terminated', 'stranded', 'stranded in booting')
        bad_exit_states     = ('stranded', 'stranded in booting')
        instances_completed = []
        while len(arrays) > 0:
            if 'seed' in arrays: arrays.remove('seed')
            instances = self.array.get_instances({'nickname': self.array_name})
            
            ## rightscale... this really sucks...
            if self.target_state == 'terminated':
                if not instances:
                    self.array.logger.info("Instances in %s terminated" % self.array_name)
                    arrays = []

            if instances:
                for i in instances:
                    if i['nickname'] not in arrays:
                        arrays.append(i['nickname'])

                    if i['state'] == self.target_state:
                        if i['nickname'] not in instances_completed:
                            self.array.logger.info("%s is %s" % (i['nickname'], i['state']))
                            instances_completed.append(i['nickname'])
                    if i['state'] in bad_exit_states:
                        if i['nickname'] not in self.instances_failed:
                            self.array.logger.warn("%s is %s" % (i['nickname'], i['state']))
                            self.instances_failed.append(i['nickname'])
                    if i['state'] in bad_exit_states or i['state'] == self.target_state:
                        arrays.remove(i['nickname'])

                    if i['nickname'] in arrays:
                        if self.progress_output:
                            self.array.logger.info("%s still building, current state: %s" % (i['nickname'], i['state']))
            if len(self.instances_failed) > 0:
                self.entry_exit_state   = 'failed'
            else:
                self.entry_exit_state   = 'operational'
                
            if len(arrays) > 0:
                time.sleep(self.sleep_time)

class AuditDNS(threading.Thread):
    def __init__(self, thread_lock, server_obj, server_name, sleep_time=5):
        threading.Thread.__init__(self)
        self.server         = server_obj
        self.server_name    = server_name
        self.thread_lock    = thread_lock
        self.blocking       = False
        self.sleep_time     = sleep_time
    def run(self):
        self.server.logger.info("Starting DNS Poll for %s" % self.server_name)
        self.thread_lock.acquire(self.blocking)
        self.poll_for_public_dns()

        if self.thread_lock.locked():
            self.thread_lock.release()

    def poll_for_public_dns(self):
        while True:
            settings = self.server.get_settings(self.server.get_id({'nickname': self.server_name}))
            if settings and 'dns-name' in settings and settings['dns-name'] != 'None' and settings['dns-name'] != None:
                self.server.logger.info("%s: DNS Information Available (%s)" % (self.server_name, settings['dns-name']))
                break
            time.sleep(self.sleep_time)

class AuditSnapshot(threading.Thread):
    def __init__(self, thread_lock, snapshot_obj, ebs_nickname, time_threshold=None, sleep_time=20, progress_output=True):
        threading.Thread.__init__(self)
        self.ebs_nickname       = ebs_nickname
        self.time_threshold     = time_threshold
        self.snapshot           = snapshot_obj
        self.thread_lock        = thread_lock
        self.blocking           = False
        self.sleep_time         = sleep_time
        self.progress_output    = progress_output
    def run(self):
        self.snapshot.logger.info("Starting Completion Poll for %s" % self.ebs_nickname)
        if self.progress_output == False:
            self.snapshot.logger.info("Progress output is turned off. This operation will be silent until finished.")
            self.snapshot.logger.info("Do note that previously completed snapshots for this volume will initially show up.")
        self.thread_lock.acquire(self.blocking)
        self.poll_for_snapshot()

        if self.thread_lock.locked():
            self.thread_lock.release()

    def poll_for_snapshot(self):
        exit_states     = ('completed')
        snaps_completed = []
        pending_snaps   = ['seed']
        while len(pending_snaps) > 0:
            if 'seed' in pending_snaps: pending_snaps.remove('seed')
            snapshots       = self.snapshot.get_details({'nickname': str(self.ebs_nickname)}, '~')
            current_snaps   = []

            if snapshots:
                for s in snapshots:
                    if self.time_threshold:
                        if s['aws_started_at'] >= self.time_threshold:
                            current_snaps.append(s)
                    else:
                        current_snaps.append(s)

            if current_snaps:
                for c in current_snaps:
                    loaded_tags     = False
                    if 'tags' in c:
                        loaded_tags = True
                        snap_position_identifier = next((item for item in c['tags'] if item['name'].find('position') != -1), None)['name']
                    else:
                        snap_position_identifier = 'position: unknown'

                    tmp_snap_name = "%s - %s" % (c['nickname'], snap_position_identifier)

                    if loaded_tags:
                        if tmp_snap_name not in pending_snaps:
                            pending_snaps.append(tmp_snap_name)

                    if c['aws_status'] in exit_states:
                        if tmp_snap_name not in snaps_completed:
                            self.snapshot.logger.info("%s: %s: %s" % (tmp_snap_name, c['aws_status'], c['aws_progress']))
                            if loaded_tags:
                                snaps_completed.append(tmp_snap_name)
                        if loaded_tags:
                            pending_snaps.remove(tmp_snap_name)

                    if tmp_snap_name in pending_snaps:
                        if self.progress_output:
                            self.snapshot.logger.info("%s: %s: %s" % (tmp_snap_name, c['aws_status'], c['aws_progress']))

            if len(pending_snaps) > 0:
                time.sleep(self.sleep_time)



