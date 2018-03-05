# coding: utf-8

from datetime import datetime
import os
import time
import logging
import MySQLdb

#####BOTS
from Spiders.HHSearch import HHSpiderSearch
from Spiders.HHView import HHSpiderView



def reopenCursor(cursor):
    cursor.close()
    return db_connection.cursor()

def get_now_time():
    return datetime.now().strftime("%d.%m.%Y %H:%M")

def get_connect():
    return MySQLdb.connect("localhost", "root", "root", "recruitment", use_unicode=True, charset='utf8')


if __name__ == '__main__':

    logger = logging.getLogger('grab')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    currentDir = os.path.dirname(os.path.abspath(__file__))

    pathLogs = os.path.join(currentDir, 'Logs')

    pathLogHHSearch = os.path.join(pathLogs, 'Search', 'HH')

    pathLogHHView = os.path.join(pathLogs, 'View', 'HH')

    # delete_files(pathLogHHSearch)
    # delete_files(pathLogHHView)

    globalStart = time.time()

    while True:
        try:
            db_connection = get_connect()
            cur = db_connection.cursor()

            ## Раскоментировать для поиска резюме#
            ###############################
             # botSearch = HHSpiderSearch(thread_number=1, network_try_limit=3, priority_mode='const')
            # logPath = pathLogHHSearch

            # botSearch.setup_grab(connect_timeout=100, timeout=120, proxy_type='http', proxy='77.246.159.237:65233',
            #                 proxy_userpwd='restyva:funny123', log_dir=logPath,
            #                 user_agent='Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36 OPR/41.0.2353.56',
            #                 follow_location=True)


        #make_log('Create bot on VIEW ------source ' + str(source_id))

        # botSearch.setup_grab(connect_timeout=100, timeout=120, proxy_type='http', log_dir=logPath,
        #                user_agent='Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36')
        # botSearch.logger = logger
        # botSearch.setup_grab(connect_timeout=100, timeout=120, proxyw_type='http', log_dir=logPath,
        #                      user_agent='Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36')
        #botSearch.logger = logger
#        botView.logger = logger

            ## Раскоментировать для просмотра резюме#
            ###############################
            botView = HHSpiderView(thread_number=1, network_try_limit=3, priority_mode='const')
            logPath = pathLogHHView

            botView.setup_grab(connect_timeout=100, timeout=120, proxy_type='http', proxy='77.246.159.237:65233',
                                proxy_userpwd='restyva:funny123', log_dir=logPath,
                                # user_agent='Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36', follow_location=True)
                                user_agent='Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.87 Safari/537.36 OPR/41.0.2353.56',
                                follow_location=True)
            botView.logger = logger
            botView.run()
            time.sleep(25)
            db_connection.close()
            time.sleep(5)
            ## закоментировать этоот блок если нужен только поиск #
            ###############################

            globalFinish = time.time()
            print 'Global time is %f' % (globalFinish - globalStart)
        except Exception as err:
            print("Some error ! ", )

