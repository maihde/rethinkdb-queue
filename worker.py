#!/usr/bin/env python
#
# vim:sw=4:sts=4:et
import rethinkdb as R
from datetime import datetime
import uuid
import time
import random
import logging

class FailoverConnection(object):
    def __init__(self, hosts, timeout=20):
        """
        @hosts string of comma separated hosts
        """
        self.connections = []
        self.timeout = timeout

        hosts = hosts.split(",")
        for host in hosts:
            if ":" in host:
                host, port = host.rsplit(":", 1)
                port = int(port)
            else:
                port = 28015
            conn = R.net.connection_type(
                host,
                port,
                db=None,
                auth_key=None,
                user='admin',
                password=None,
                timeout=self.timeout,
                ssl=dict(),
                _handshake_version=10
            )
            self.connections.append(conn)
            logging.debug("Created connection %s:%s", host, port)

    def _start(self, term, **global_optargs):
        e = None
        # Try at most each connection once
        for i in xrange(len(self.connections)):
            # The preferred connection is always at the head of the list
            conn = self.connections[0]
            try:
                # There are a variety of failure scenarios
                # where conn._instance._socket is None when
                # calling methods such as is_open() or reconnect()
                # Now check if the connection is open 
                if not conn.is_open():
                    logging.debug("Reconnecting %s:%s", conn.host, conn.port)
                    conn.reconnect(timeout=self.timeout)
                return conn._start(term, **global_optargs)
            except R.errors.ReqlError, e:
                logging.info("Connection %s:%s failed due to %s", conn.host, conn.port, e)
                try:
                    conn.close()
                except:
                    pass
                # if it fails, we pop it off and append to the end
                # (i.e. we rotate all the connections
                self.connections.append(self.connections.pop(0))
                logging.info("Now using %s:%s",
                    self.connections[0].host, self.connections[0].port)
        # The only way we get here is if every connection had
        # an exception, if so we rethrow it otherwise we throw
        # our own error
        if e == None:
            raise e
        else:
            raise R.errors.ReqlError("All connections failed")

class RethinkDBWorker(object):
    def __init__(self, **kwargs):
        self.opts = kwargs

    def work_forever(self):
        conn = FailoverConnection(self.opts["hosts"])
        
        T = R.table(self.opts["table"])

        # Create the table
        existing_tables = R.db(self.opts["db"]).table_list().run(conn)
        if self.opts["table"] not in existing_tables:
            logging.info("Creating table %s within db %s", self.opts["table"], self.opts["db"])
            R.db(
                self.opts["db"]
            ).table_create(
                self.opts["table"]
            ).run(
                conn
            )

        #===============================
        # FEEDER
        if self.opts["feeder"]:
            insert_delay = 1.0 / self.opts["rate_hz"]
            logging.info("Starting feeder with insert rate %s jobs/sec", self.opts["rate_hz"])
            while True:
                # Wait until we need to send the next message
                next_message = time.time() + insert_delay
                while time.time() < next_message:
                    time.sleep(min(0.1, insert_delay))

                try: 
                    job_uid = str(uuid.uuid4())
                    logging.debug("Publishing job %s", job_uid)
                    T.insert({
                         "timestamp": datetime.now(R.make_timezone("00:00")), # datetime objects in Rethink *MUST* contains timezone information
                         "job_uid": job_uid,
                         "state": self.opts["input_state"]
                    }).run(
                        conn
                    )
                    logging.debug("Published job %s", job_uid)
                except R.errors.ReqlOpFailedError, e:
                    logging.exception("Encountered ReqlOpFailedError, job has not been inserted")
                    # this logic simply skips the job...but obviously in some scenarios you would 
                    # want to continue to try and reinsert the job
                    continue 
 

                try: 
                    # See how many are pending
                    pending = T.filter(
                        R.row['state'] == self.opts["input_state"]
                    ).count(
                    ).run(
                        conn
                    )
                    logging.info("There are %s jobs pending", pending)
                except R.errors.ReqlOpFailedError, e:
                    logging.exception("Encountered ReqlOpFailedError counting pending")
                
        #===============================
        # WORKER
        else:
            # Create the changefeed query
            while True:
                try:
                    feed = T.filter(
                        R.row['state'] == self.opts["input_state"]
                    ).changes(
                        include_initial=True
                    ).run(
                        conn
                    )
                    
                    logging.info("Starting worker with service rate %s jobs/sec", self.opts["rate_hz"])
                    items_worked = 0
                    backlog_cnt = 0
                    for item in feed:
                        # If old_val doesn't exist, then this is an entry that
                        # matched the query as part of the include_initial.  If 
                        if item.get("old_val") == None:
                            backlog_cnt += 1
                        elif item.get("new_val") == None:
                            # We get an event of new_val None when the state is switched
                            # to a value other than STATE_1...i.e. when new_val is None
                            # that means that a document that used to match our
                            # change feed filter no longer does
                            continue
                        else:
                            logging.info("Processed %s backlog entries, now running realtime", backlog_cnt)

                        # Conditionally update the record
                        result = T.get(
                            item["new_val"]["id"]
                        ).update(
                             R.branch(
                                 R.row['state'] == self.opts["input_state"],
                                 {"state": self.opts["output_state"]},
                                 None
                             )
                        ).run(
                            conn
                        )

                        if not result.get("replaced"):
                            # if the job wasn't replaced, then this worker didn't acquire it
                            # and the job was handled by someone else
                            continue

                        logging.debug("WORKING %s", item)
                        items_worked += 1
                        time.sleep(1.0/self.opts["rate_hz"]) # simulate some amount of work actually being done
                        logging.info("Items Worked %s", items_worked)
                
                except R.errors.ReqlError, e:
                    logging.exception("Encountered ReqlOpFailedError, trying to reestablish changefeed")
                    time.sleep(0.1) # prevent spinning
                   

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--hosts", default="localhost:28015")
    parser.add_option("--db", default="test")
    parser.add_option("--table", default="queue_test")
    parser.add_option("--feeder", default=False, action="store_true")
    parser.add_option("-r", "--rate-hz", default=1, type="int")
    parser.add_option("--input-state", default="STATE_1")
    parser.add_option("--output-state", default="STATE_2")
    parser.add_option("-d", "--debug", default=0, action="count")
    opts, args = parser.parse_args()

    logging.basicConfig(level=logging.WARN)
    if opts.debug == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif opts.debug >= 2:
        logging.getLogger().setLevel(logging.DEBUG)

    w = RethinkDBWorker(**opts.__dict__)
    w.work_forever()
