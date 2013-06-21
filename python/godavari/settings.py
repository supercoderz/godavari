#get the basics
import logging

#The settings for the application
#default port of the node
PORT=1111
#default port of the master
MASTER_PORT=2207
#localhost
HOST='localhost'
#Max number of threads started
MAX_THREADS=20
#The interval between heartbeat checks - secs
HEART_BEAT_CHECK_INTERVAL=10
#The time delay after which the proxy is removed if
#there is no heartbeat - secs
HEART_BEAT_EXPIRY=10
#The node heart beat interval - this is less than the
#heart beat expiry - secs
NODE_HEART_BEAT_INTERVAL=9

#states of a job
JOB_RUNNING='RUNNING'
JOB_WAITING_TO_RUN='WAITING TO RUN'
JOB_COMPLETED='COMPLETED'

#state database settings
DB_PATH='./godavari.db'
DB_INIT_CHECK_QUERY='select * from jobs'
JOBS_TABLE_CREATE_SQL='create table jobs(job_id text,running_on text,created_at text,state text,completed_at text)'
JOB_INSERT='insert into jobs values(?,?,?,?,?)'
JOB_UPDATE_STATE='update jobs set state=? where job_id=?'
JOB_UPDATE_SERVER='update jobs set running_on=? where job_id=?'
JOB_UPDATE_COMPLETED='update jobs set completed_at=? where job_id=?'
RUNNING_JOBS="select * from jobs where state='RUNNING'"
COMPLETED_JOBS="select * from jobs where state='COMPLETED'"
WAITING_JOBS="select * from jobs where state='WAITING TO RUN'"
CLEANUP_SQL="delete from jobs where completed_at < CAST(? as TEXT)"
DB_TIMEOUT=120
#This is in days
COMPLETED_JOB_CLEAN_DELAY=2
#seconds
DATABASE_CLEANER_SLEEP=120

#web server
RUNNING_PATH="running.html"
WAITING_PATH="waiting.html"
COMPLETED_PATH="completed.html"
WEB_PORT=1110
HEADER="""<html><head>
              <title>Godavari</title>
              <style type="text/css">
              table.sample {
                    border-width: 1px;
                    border-spacing: 2px;
                    border-style: solid;
                    border-color: gray;
              }
              table.sample th {
                    border-width: 1px;
                    padding: 2px;
                    background-color: FFCC00;
              }
              table.sample td {
                    border-width: 1px;
                    padding: 2px;
                    background-color: FFCC99;
              }

              div.header {
                    background-color:99CCFF;
              }
              </style>
              </head>
              <body>
              <div class='header'><h2>Godavari</h2><hr></div>
              <div>"""
INDEX_PAGE="""<a href='waiting.html'>Waiting Jobs</a><br>
              <a href='running.html'>Running Jobs</a><br>
              <a href='completed.html'>Completed Jobs</a><br>
              </div>
              </body>
              </html>
           """
HTML_START="""<table class='sample'>
              <tr>
              <th>ID</th><th>Host</th>
              <th>Created At</th><th>Status</th>
              <th>Completed At</th>
              </tr>"""
HTML_END="</table></div></body></html>"
TABLE_ROW="<tr><td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> <td>%s</td> </tr>"

#Configure the logging system
FORMAT = '%(asctime)-15s %(levelname)s %(module)-8s %(threadName)s %(message)s'
logging.basicConfig(format=FORMAT,level=logging.DEBUG)
logger=logging.getLogger('godavari')

