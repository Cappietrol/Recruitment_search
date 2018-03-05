# coding: utf-8

from grab.spider import Spider, Task
#from grab.error import DataNotFound

import MySQLdb
import datetime
import time
import random
import re
import json



class HHSpiderSearch(Spider):
    initial_urls = ['https://Google.com']
    database = "recruitment"
    count = 0
    count_resume = 0
    source_id = 1
    commonGrab = ''
    db = ''
    cur = ''
    logger = ''
    clients = ''
    max_request = 0
    id_login = None
    limit_request_done = False
    cookiesPathes = None

    def source_is_full(self):
        return self.limit_request_done

    def close_connect(self):
        self.db.close()

    def logout(self, g):
        print("logout")

    def set_login_status_busy(self, is_busy, id_login):
        self.reopenCursor()

        self.cur.execute(""""
            Update 
                recruitment.logins 
            set 
                last_use_date = now(), 
                is_busy = '{0}' 
            WHERE 
                id = {1};""".format (is_busy, id_login) )
        self.cur.execute("commit;")


    def get_login(self, is_busy):
        print('Get login. IS busy {0}'.format(is_busy))
        time.sleep(1)
        query = ("""
                    SELECT 
                        login.id, 
                        login.login, 
                        login.password, 
                        login.count_page, 
                        login.count_resume, 
                        s.max_request, 
                        case 
				            WHEN (DATE_SUB(CURDATE(), INTERVAL 2 DAY)) <= date(login.date_cookie)
				            THEN login.cookie 
					        ELSE null end as cookies 
                    FROM 
                        recruitment.logins login 
                    LEFT JOIN 
                        recruitment.SOURCE s on (s.id = login.SOURCE_ID) 
                    WHERE 
                	    login.is_active = 'Y' 
                	        and login.is_busy = '{0}' 
                	        and login.source_id = {1} 
                            and (login.count_page < s.max_request) 
                    ORDER BY login.is_paid limit 1;""".format(is_busy, str(self.source_id)))

        self.reopenCursor()
        self.cur.execute(query)
        return self.cur.fetchone()

    def update_cookie(self, cookie):
        print('Save cookie')
        query = """ Update 
                        recruitment.logins 
                    SET 
                        cookie = replace(replace(replace('{0}', '\"http:', '\\\"http:'), '\"https:', '\\\"https:'), '.ru/\"', '.ru/\\\"'),
                        date_cookie = now() 
                    WHERE 
                        id = {1}; 
                    commit;""".format(cookie, self.id_login)

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
        self.count_resume = row[4]
        self.max_request = row[5]
        login = row[1]
        passwd = row[2]
        print("---------LOGIN ON--------------- " + str(login) + " id " + str(self.id_login))
        g.go('https://spb.hh.ru/account/login')
        g.set_input('username', login)
        g.set_input('password', passwd)
        g.submit()
        self.set_login_status_busy('Y', self.id_login)

        return g


    def shutdown(self):
        self.update_login_info()
        self.logger.debug('Job done!')

    def task_initial(self, grab, task):

        self.db = self.getConnection()
        self.cur = self.db.cursor()
        self.count = 1

        g = grab.clone()
        #g.cookies.clear()
        row = self.get_login('Y')

        if row == None:
            print 'login'
            g = self.login(g)
            self.update_cookie(json.dumps(g.cookies.get_dict()))
        elif row[6] == None:
            print 'login'
            self.set_login_status_busy('N', int(row[0]))
            g = self.login(g)
            self.update_cookie(json.dumps(g.cookies.get_dict()))
        else:
            print '-----else----'
            self.id_login = int(row[0])
            self.count = row[3]+row[4]
            self.count_resume = row[4]
            self.max_request = row[5]
            cokkie = str(row[6]).replace(': \"\",', 'dvkov').replace(': \"\"', ': \"(-+)').replace('\"\", ', '(+-)\", ').replace('dvkov', ': \"\",')
            items = json.loads(cokkie)
            for item in items:
                for key in item.keys():
                    if item[key] and type(item[key]) is unicode:
                        item[key] = item[key].replace('(+-)','"').replace('(-+)','"')
            g.cookies.load_from_json(items)
        print 'sleep'
        time.sleep(5)

        print("---------LOGIN COMPLETE--------------- ")
        for i in range(0, 13):

            url = 'https://irkutsk.hh.ru/search/resume?text=call-%D1%86%D0%B5%D0%BD%D1%82%D1%80%2C+%D0%B1%D0%B0%D0%BD%D0%BA%2C+%D0%BF%D1%80%D0%BE%D0%B4%D0%B0%D0%B6%D0%B0%D0%BC%2C+%D0%92%D0%B7%D1%8B%D1%81%D0%BA%D0%B0%D0%BD%D0%B8%D0%B5%2C+%D0%BA%D0%BE%D0%BB%D0%BB+%D1%86%D0%B5%D0%BD%D1%82%D1%80%2C+%D0%BA%D0%BE%D0%BB%D0%BB-%D1%86%D0%B5%D0%BD%D1%82%D1%80%2C+%D0%9A%D0%BE%D0%BD%D1%82%D0%B0%D0%BA%D1%82-%D1%86%D0%B5%D0%BD%D1%82%D1%80&logic=any&pos=position&exp_period=all_time&text=%D0%BA%D0%B0%D1%81%D1%81%D0%B8%D1%80%2C+%D0%BE%D1%84%D0%B8%D1%81-%D0%BC%D0%B5%D0%BD%D0%B5%D0%B4%D0%B6%D0%B5%D1%80%2C+%D0%B0%D0%B4%D0%BC%D0%B8%D0%BD%D0%B8%D1%81%D1%82%D1%80%D0%B0%D1%82%D0%BE%D1%80%2C+%D1%80%D1%83%D0%BA%D0%BE%D0%B2%D0%BE%D0%B4%D0%B8%D1%82%D0%B5%D0%BB%D1%8C&logic=except&pos=full_text&exp_period=all_time&area=75&area=2&relocation=living&salary_from=&salary_to=35000&currency_code=RUR&education=none&age_from=22&age_to=42&gender=unknown&employment=full&order_by=publication_time&source=all&search_period=0&items_on_page=100'

            g.setup(url=url, user_agent='Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36')
            config = g.dump_config()
            self.commonGrab = g
            if not self.check_login():
                break
            time.sleep(random.randint(5, 13))
            self.count += 1
            print("PAGE "+ str(i))
            yield Task('resume', use_proxylist=False, grab_config=config, disable_cache=True, delay=random.randint(2, 5))
        print("---------FILLING COMPLETE --------------- ")

    def task_resume(self, grab, task):
        if grab.response.code == 200:
            self.commonGrab = grab
            resumes = grab.tree.xpath('//table[@class="output"]//tr//td/div[@class="output__main"]/..')
            for resume in resumes:
                Fullname = resume.xpath('./div[@class="output__main"]/div[@class="output__fullname"]/text()')
                print(Fullname)
                if len(Fullname) > 0:
                    Fullname = Fullname[0].upper()
                    if Fullname.replace(' ','') == '':
                        continue
                    Age = re.sub("\D", "", resume.xpath('./div[@class="output__info"]/span[@class="output__age"]/text()')[0])
                    if Age.replace(' ', '') == '':
                        Age = '0'

                    Updated = resume.xpath('./div[@class="output__addition"]/span[@class="output__tab m-output__date"]/text()')[0]
                    Updated = Updated.split(',')[0].split('  ')[1].split()
                    resume_title = (resume.xpath('./div[@class="output__main"]/span/a/text()')[0]).upper()




                    if len(Updated) == 2:
                        Updated = datetime.date(datetime.date.today().year, self.getMonth(Updated[1]), int(Updated[0])).isoformat()
                    elif len(Updated) == 3:
                        Updated = datetime.date(int(Updated[2]), self.getMonth(Updated[1]), int(Updated[0])).isoformat()

                    url = grab.make_url_absolute(resume.xpath('./div[@class="output__main"]/span/a/@href')[0], resolve_base=True)
                    url = url[:url.find('?query')]
                    resume_title = (resume.xpath('./div[@class="output__main"]/span/a/text()')[0]).encode('utf-8')
                    indents = resume.xpath('./div[@class="output__main"]/div[@class="output__indent"]/text()')
                    title_indents = resume.xpath('./div[@class="output__main"]/div[@class="output__indent"]/text()')
                    
                    AREA = u'null'
                    for index, title in enumerate(title_indents):
                        if title == u'Регион':
                            AREA = indents[index]
                            break

                    LNAME = Fullname.split()[0]
                    FNAME = Fullname.split()[1]
                    if len(Fullname.split()) == 3:
                        MNAME = Fullname.split()[2]
                    else:
                        MNAME = ''

                    self.reopenCursor()

                    #decode('utf-8')
                    query = """ INSERT INTO 
                                    recruitment.`resume`
                                        (`URL`, 
                                        `CLIENT_ID`, 
                                        `FNAME`,
                                        `LNAME`,
                                        `MNAME`,
                                        `AGE`,
                                        `AREA`,
                                        `SOURCE_ID`,
                                        `RESUME_UPDATED`, 
                                        `CAN_WRITE_MESS`, 
                                        `resume_title`)
                                values
                                    ('{0}',{1},'{2}','{3}','{4}',{5},{6},{7},'{8}','', '{9}');""".format(
                                    url.encode('utf-8'), 1, FNAME.encode('utf-8'), LNAME.encode('utf-8'),
                                    MNAME.encode('utf-8'), str(Age).encode('utf-8'), AREA.encode('utf-8'),
                                    str(self.source_id).encode('utf-8'),
                                    Updated.encode('utf-8'), resume_title)
                    self.cur.execute(query)
                    self.cur.execute("commit;")

        else:
            print 'grab.response.code<>200'

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
        print("---------Update login--------------- " + str(self.count) +" (" + str(self.max_request) + ")" )
        print("Id " + str(self.id_login))
        query = "UPDATE recruitment.logins SET count_page = {0} where id={1};".format(str(self.count), str(self.id_login))
        self.reopenCursor()
        self.cur.execute(query)
        self.cur.execute("commit;")

    def check_login(self):
        if self.count > self.max_request:
            self.limit_request_done = True
            return False
        return True

    def getConnection(self):
        return MySQLdb.connect("localhost", "root", "root", "recruitment", use_unicode=True, charset='utf8')