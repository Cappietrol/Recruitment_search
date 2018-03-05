# coding: utf-8
import os
import random
from grab.spider import Spider, Task
#from grab.error import DataNotFound

import MySQLdb
import datetime
import time
import urllib
import re
import json

class HHSpiderView(Spider):
    initial_urls = ['https://spb.hh.ru']
    database = "recruitment"
    count = 0
    source_id = 1
    db = ''
    cur = ''
    logger = ''
    clients = ''
    max_request = 0
    id_login = 0
    count_request = 0
    count_view = 0
    max_resume_view = 0

    commonGrab = ''

    limit_request_done = False
    cookiesPath = None

    def set_login_status_busy(self, is_busy):
        self.reopenCursor()

        self.cur.execute("""
                            Update 
                                recruitment.logins 
                            set 
                                last_use_date = now(), 
                                is_busy = %s 
                            WHERE id = %s;""", (is_busy, self.id_login))
        self.cur.execute("commit;")

    def get_login(self):
        self.logger.info('Get login')
        time.sleep(1)
        query = ("""
                    SELECT
                	    login.id, 
                	    login.login, 
                	    login.password, 
                	    login.count_page, 
                	    login.count_resume, 
                	    s.max_resume_view, 
                        case 
						    WHEN 
						        (DATE_SUB(CURDATE(), INTERVAL 2 DAY)) <= date(login.date_cookie) 
						    THEN 
						        login.cookie 
					        ELSE 
					            null end as cookies
                    FROM 
                        recruitment.logins login
                    LEFT JOIN 
                        recruitment.SOURCE s on (s.id = login.SOURCE_ID) 
                    where
                        login.is_active = 'Y'
                        and login.is_paid = 'Y'
                        and login.source_id = {0}
                        and (login.count_resume < s.max_resume_view)
                    order by login.is_busy desc 
                    limit 1;""").format(self.source_id)

        self.reopenCursor()
        self.cur.execute(query)
        return self.cur.fetchone()

    def update_cookie(self, cookie):
        self.logger.info('Save cookie')
        query = """
                    Update 
                        recruitment.logins 
                    SET 
                    cookie = replace(replace(replace('{0}', '\"http:', '\\\"http:'), '\"https:', '\\\"https:'), '.ru/\"', '.ru/\\\"'), date_cookie = now() WHERE id = {1}; 
                    commit;""".format(
                                        cookie, self.id_login)
        self.reopenCursor()
        print '----------------------------------------'
        print query
        print '----------------------------------------'
        self.cur.execute(query)

    def login(self, g):
        row = None
        while row is None:
            row = self.get_login('N')

        self.id_login = int(row[0])
        self.count = row[3]+row[4]
        self.count_view = row[4]
        self.max_resume_view = row[5]
        login = row[1]
        passwd = row[2]
        self.logger.info("---------LOGIN ON--------------- " + str(login) + " id " + str(self.id_login))
        g.go('https://spb.hh.ru/account/login')
        g.set_input('username', login)
        g.set_input('password', passwd)
        g.submit()
        self.set_login_status_busy('Y')

        return g

    def get_urls(self):
        query = "Select id,URL from recruitment.resume where SOURCE_ID = {0} and VIEW_DATE is null order by resume_updated desc limit 10".format(self.source_id)
        self.cur.execute(query)
        return self.cur

    def shutdown(self):
        self.update_login_info()
        self.logger.debug('Job done!')

    def task_initial(self, grab, task):
        self.db = self.getConnection()
        self.cur = self.db.cursor()
        self.count = 1

        g = grab.clone()
        row = self.get_login('Y')

        if row == None:
            print 'New login'
            g = self.login(g)
            self.update_cookie(json.dumps(g.cookies.get_dict()))
        elif row[6] == None:
            print 'New login'
            g = self.login(g)
            self.update_cookie(json.dumps(g.cookies.get_dict()))
        else:
            print 'Login with cokkie'
            print 'Login is ' + row[1]
            self.id_login = int(row[0])
            self.count = row[3]+row[4]
            self.count_view = row[4]
            self.max_resume_view = row[5]
            cokkie = str(row[6]).replace(': \"\",', 'dvkov').replace(': \"\"', ': \"(-+)').replace('\"\", ', '(+-)\", ').replace('dvkov', ': \"\",')
            items = json.loads(cokkie)
            for item in items:
                for key in item.keys():
                    if item[key] and type(item[key]) is unicode:
                        item[key] = item[key].replace('(+-)','"').replace('(-+)','"')
            g.cookies.load_from_json(items)
        print 'sleep'
        time.sleep(5)

        self.logger.info("---------LOGIN COMPLETE--------------- ")

        for index, (resume_id, url) in enumerate(self.get_urls()):
            g.setup(url=url)
            config = g.dump_config()

            if not self.check_login():
                break
            time.sleep(random.randint(10, 15))

            self.cur.execute("Update recruitment.`resume` set VIEW_DATE = now() where id = {0} and source_id = {1}".format(resume_id, self.source_id))
            self.cur.execute("commit;")
            yield Task('page', use_proxylist=False, resume_id=resume_id, priority=index, grab_config=config,  disable_cache=True, delay=random.randint(2, 5))

    def task_page(self, grab, task):
        time.sleep(random.randint(10, 15))
        try:
            g = grab
            self.logger.info(g.cookies.cookiejar._cookies[u'spb.hh.ru'][u'/'][u'hhrole'].value)
            strInfo = {}
            #Get PHONE
            #dispNew = ''
            #dispBelong = ''
            #newPhones = ''
            #belongPhones = ''
            Phone = ''
            emails = ''
            phones = []
            phoneFromSite = g.tree.xpath('//span[@itemprop = "telephone"]/text()')

            for phone_number in phoneFromSite:
                Phone = re.sub("\D", "", phone_number)
                phones.append(Phone[1:])

            strInfo['resume_id'] = task.resume_id
            strInfo['phones'] = phones

            #Get email

            email = g.tree.xpath('//a[@itemprop="email"]/text()')
            disp = ''

            for em in email:
                emails += disp + em
                disp = ', '

            strInfo['email'] = emails[:199]

            #Get skype
            disp = ''
            skypes = ''
            skype = g.tree.xpath('//span[@class="resume__contacts__personalsite siteicon m-siteicon_skype"]/text()')
            for sk in skype:
                 skypes += disp + sk
                 disp = ', '
            strInfo['skype'] = skypes[:199]

            #Get AREA
            area = g.tree.xpath('//span[@itemprop="addressLocality" and @data-qa="resume-personal-address"]//text()')
            if len(area) != 0:
                try:
                    strInfo['area'] = area[0].replace("'", '')
                except:
                    strInfo['area'] = area[0]
            else:
                strInfo['area'] = ''

            #Get resume_updated
            try:
                resume_updated = g.tree.xpath('//span[@class="resume__updated"]/text()')
                if len(resume_updated) <> 0:
                    strInfo['resume_updated'] = datetime.datetime.strptime(re.findall('\d\d.\d\d.\d\d\d\d', resume_updated[0])[0], '%d.%m.%Y').isoformat()
                else:
                    strInfo['resume_updated'] = ''
            except:
                strInfo['resume_updated'] = ''

            #Get WORKPLACES
            workPlaces = ''
            disp = ''
            work = g.tree.xpath('//div[@class="resume-block" and @data-qa="resume-block-experience"]//div[@itemprop="name" and @class="resume-block__sub-title"]/text()')
            for workPlace in work:
                workPlaces += disp + workPlace.replace("'", '')
                disp = ', '
            strInfo['work_info'] = workPlaces[:1499]

            #Get TEACHPLACES
            teachPlaces = ''
            disp = ''
            education = g.tree.xpath('//div[@class="resume-block" and @data-qa="resume-block-education"]//text()')
            for teachPlace in education:
                teachPlaces += disp + teachPlace.replace("'", '')
                disp = ', '
            strInfo['teach_info'] = teachPlaces[:1499]

            #Get BIRTHDAY V
            try:
                birthSelect = g.doc.select('//span[@data-qa="resume-personal-age"]/..//text()')[3].text().split(' ')
                dateB = ''
                if len(birthSelect) == 5:
                    strInfo['birthday'] = datetime.date(
                        int(g.doc.select('//span[@data-qa="resume-personal-age"]/..//text()')[3].text().split(' ')[4]),
                        self.getMonth(
                            g.doc.select('//span[@data-qa="resume-personal-age"]/..//text()')[3].text().split(' ')[3]),
                        int(g.doc.select('//span[@data-qa="resume-personal-age"]/..//text()')[3].text().split(' ')[
                                2])).isoformat()
                else:
                    strInfo['birthday'] = '0001-01-01'
                    print 'Birthday is fined, but is null'

            except:
                strInfo['birthday'] = '0001-01-01'
                print 'birthday is null'


            self.reopenCursor()
            self.cur.execute("""
                                INSERT INTO 
                                    recruitment.resume_info \n\
                                    (`resume_id`, 
                                    `birthday`, 
                                    `work_info`, 
                                    `teach_info`, 
                                    `email`, 
                                    `skype`,
                                    `area`,
                                    `resume_updated_by_worker`) \n\
                                VALUES 
                                    ({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}');""".format(
                        strInfo['resume_id'],
                        strInfo['birthday'],
                        #'нет' if strInfo['work_info'].encode('utf8')=='' else 'off',
                        strInfo['work_info'].encode('utf8'),
                        strInfo['teach_info'].encode('utf8'),
                        strInfo['email'].encode('utf8'),
                        strInfo['skype'].encode('utf8'),
                        strInfo['area'].encode('utf8'),
                        strInfo['resume_updated']))


            for phone in phones:
                self.cur.execute("INSERT INTO recruitment.phones_from_resume (`resume_id`,`phone`) values({0},'{1}');".format(strInfo['resume_id'], phone))
            self.cur.execute('Commit;')
        except:
            self.logger.info('------ERROR----- ' + str(strInfo['resume_id']))

    def reopenCursor(self):
        self.cur.close()
        self.cur = self.db.cursor()

    def getMonth(self, month_name):

        if month_name == u'января' or month_name == u'янв':
            return 1
        elif month_name == u'февраля' or month_name == u'фев':
            return 2
        elif month_name == u'марта' or month_name == u'мар':
            return 3
        elif month_name == u'апреля' or month_name == u'апр':
            return 4
        elif month_name == u'мая':
            return 5
        elif month_name == u'июня' or month_name == u'июн':
            return 6
        elif month_name == u'июля' or month_name == u'июл':
            return 7
        elif month_name == u'августа' or month_name == u'авг':
            return 8
        elif month_name == u'сентября' or month_name == u'сен':
            return 9
        elif month_name == u'октября' or month_name == u'окт':
            return 10
        elif month_name == u'ноября' or month_name == u'ноя':
            return 11
        elif month_name == u'декабря' or month_name == u'дек':
            return 12
        return

    def update_login_info(self):
        self.logger.info("---------Update login--------------- " + str(self.count_view) +" (" + str(self.max_resume_view) + ")" )
        query = ("UPDATE recruitment.logins SET count_resume={0} where id={1};".format(str(self.count_view), str(self.id_login)) )
        self.reopenCursor()
        self.cur.execute(query)
        self.cur.execute("commit;")

    def check_login(self):
        self.count_view += 1
        if self.count_view > self.max_resume_view:
            self.limit_request_done = True
            print 'return false'
            return False
        return True


    def getConnection(self):
        return MySQLdb.connect("localhost", "root", "root", "recruitment", use_unicode=True, charset='utf8')