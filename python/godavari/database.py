#import the basics
import sqlite3,time
from datetime import datetime,timedelta
import threading
#import the settings
from settings import *


#check and create database tables if required
def create_tables():
    state_db=sqlite3.connect(DB_PATH)
    state_db.isolation_level=None
    try:
        #try to get the state
        state_db.cursor().execute(DB_INIT_CHECK_QUERY)
    except:
        #if we are here then there is no table
        #create the table
        logger.info('Found empty DB.Initializing DB for use.')
        state_db.cursor().execute(JOBS_TABLE_CREATE_SQL)

#This avoids threads getting locked up later
create_tables()


"""
DbManager -

This is used to create the state DB which is used by the web interface
"""
class DbManager:
    """
    init_db -

    This initializes the state database 
    """
    def init_db(self):
        self.state_db=sqlite3.connect(DB_PATH,timeout=DB_TIMEOUT)
        self.state_db.isolation_level=None

    def close(self):
        self.state_db.close()

    def create(self,job_id):
        self.state_db.cursor().execute(JOB_INSERT,(job_id,'TBD',str(datetime.now()),JOB_WAITING_TO_RUN,'NA'))

    def set_started(self,job_id,url):
        self.state_db.cursor().execute(JOB_UPDATE_SERVER,(url,job_id))
        self.state_db.cursor().execute(JOB_UPDATE_STATE,(JOB_RUNNING,job_id))

    def job_complete(self,job_id):
        self.state_db.cursor().execute(JOB_UPDATE_STATE,(JOB_COMPLETED,job_id))
        self.state_db.cursor().execute(JOB_UPDATE_COMPLETED,(str(datetime.now()),job_id))


"""
This thread cleans the old completed jobs
"""
class DatabaseCleaner(threading.Thread):
    def __init__(self):
        super(DatabaseCleaner, self).__init__()
        self.stop = threading.Event()

    def run(self):
        try:
            while not self.stop.isSet():
                logger.info("Running cleanup thread to remove all completed jobs older than %s days"%(COMPLETED_JOB_CLEAN_DELAY))
                #get the date beyong which clean up is needed
                t=timedelta(days=COMPLETED_JOB_CLEAN_DELAY)
                clean_date=datetime.now()-t
                #now connect, clean and commit
                db=sqlite3.connect(DB_PATH)
                db.cursor().execute(CLEANUP_SQL,(clean_date,))
                db.commit()
                db.close()
                #now wait for few seconds
                time.sleep(DATABASE_CLEANER_SLEEP)
            logger.info('Stopping database cleaner manager')
        except KeyboardInterrupt,SystemExit:
            logger.info('Stopping database cleaner manager')
            raise
