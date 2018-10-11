# -*- coding: utf-8 -*-
import scrapy,regex,os,json,logging,pandas
from scrapy.selector import Selector
from urllib.parse import urlencode
from Quora_Mining.SQL import SQL
from scrapy.utils.project import get_project_settings
from datetime import timedelta,datetime

#Convert Quora time duration to python time delta
def quora_timedelta(timestring):
    unit_delta={'m':timedelta(minutes=1),
                'h':timedelta(hours=1),
                'd':timedelta(days=1),
                'w':timedelta(weeks=1)}
    try:
        unit=regex.compile(r'(?<=\d*?)\D(?=\sago)',regex.IGNORECASE).search(timestring).group()
        quant=regex.compile(r'\d*?(?=\D*?$)',regex.IGNORECASE).search(timestring).group()
    except:
        return timedelta(hours=0)

    return int(quant)*unit_delta[unit]


class QuestionScanSpider(scrapy.Spider):
    name = 'Question_Scan'

    # Question_file
    main_domain=r'https://www.quora.com'
    login_api = r"https://www.quora.com/webnode2/server_call_POST?_h=Vn03YsuKFZvHV9&_m=do_login"
    next_log_api=r"https://www.quora.com/webnode2/server_call_POST?_h=h0Iys3ulhTPzkz&_m=increase_count"
    next_log_url = r"https://tch229035.tch.quora.com/up/%s/updates?&min_seq=%s&channel=%s&hash=%s&timeout=2000"

    # Request parameters
    user='largomath2010@gmail.com'
    password='MeoYeu@0110'
    login_json = {"args": [], "kwargs": {"email": user, "password": password}}
    next_log_json = '{"args":[],"kwargs":{"cid":"%s","num":20,"current":%s}}'

    header_request={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'}
    login_load={'json':json.dumps(login_json), 'formkey': '', 'postkey': '', 'revision': '', 'window_id': '', '__method': 'do_login',
                '__hmac': 'Vn03YsuKFZvHV9', 'referring_action': 'index', 'referring_controller': 'index', '__metadata': '{}', 'js_init': '{}'}

    next_log_load = dict(json='', revision='', formkey='', postkey='', window_id='', referring_controller='question',
                         referring_action='log', __hmac='h0Iys3ulhTPzkz', __method='increase_count',__first_server_call='true',
                         js_init='{"object_id":9927064,"initial_count":20,"buffer_count":20,"crawler":false,"has_more":true,"retarget_links":true,"fixed_size_paged_list":false,"auto_paged":true}')



    # Regex tool
    key_pttr = r'(?<=\"{0}\"\:\s\")[^\"]*?(?=\")'
    key_token = lambda r,x,y,k: regex.compile(k.format(x), regex.IGNORECASE).search(y).group()
    get_window_id = regex.compile(r'(?<=window_id\s\=\s\")[^\"]*?(?=\")', regex.IGNORECASE)
    get_qid = regex.compile(r'(?<=\"qid\"\:\s).*?(?=\}\,)', regex.IGNORECASE)
    get_cid = regex.compile(r'(?<=\")[^\"]*?(?=\"\:\s\{\"more_button\"\:)', regex.IGNORECASE)
    get_htmls = regex.compile(r'(?<=\\"html\\"\:\s\\").*?(?=\\"\,\s\\"css\\")', regex.IGNORECASE)
    get_min_seq = regex.compile(r'(?<=\"min\_seq\"\:).*?(?=\})', regex.IGNORECASE)

    get_log_chan = regex.compile(r'(?<=\")chan[^\"]*?(?=\"\,\s\"quora\.com)', regex.IGNORECASE)
    get_log_hash = regex.compile(r'(?<=")[^\"]*?(?=\"\,\s\"chan)', regex.IGNORECASE)
    get_log_channel = regex.compile(r'(?<=start\(0\,\s\")[^\"]*?(?=\")', regex.IGNORECASE)


    # Other tools
    settings = get_project_settings()
    db_path=settings.get('DB_PATH')
    questions=settings.get('QUESTIONS')
    answers = settings.get('ANSWERS')
    askers=settings.get('ASKERS')
    SQLITE={'QUESTIONS':SQL(db_path=db_path, db_table=questions),
            'ANSWERS':SQL(db_path=db_path, db_table=answers),
            'ASKERS':SQL(db_path=db_path, db_table=askers)}

    inc_const = 20
    init_const = 20
    next_log_meta_keys=['cid', 'hash', 'channel', 'chan','current_sum','min_seq','main_item','next_log_load']
    custom_settings = {
        'ITEM_PIPELINES':{
            'Quora_Mining.pipelines.Answers_Or_Askers':50,
        },
    }


    # List css:
    logs_css=r'.LogOperation.PagedListItem'
    next_page_check=r'.pager_next.action_button:not(.hidden)'
    question_add_check=r'.feed_item_activity::text'

    answer_css_dict={'answer_url':lambda response:response.css(r'.feed_item_activity>a::attr(href)').extract_first(),
                     'answer_id':lambda response:response.css(r'.log_action_bar>a[href]::text').extract_first(),
                     'answer_timestamp':lambda response:(datetime.now()-quora_timedelta(response.css(r'.log_action_bar>.datetime::text').extract_first())).strftime('%d-%m-%Y %H:%M:%S'),
                     'answerer_name':lambda response:response.css(r'span>a.user::text').extract_first(),
                     'answerer_url': lambda response: response.css(r'span>a.user::attr(href)').extract_first(),
                     }

    detail_css_dict={'answer_upvotes':lambda response: response.css('[action_target*=aid]>div>div>span>span>span::text').extract_first(),
                     'answer_views':lambda response: response.css('.ContentFooter.AnswerFooter>span>span.meta_num::text').extract_first(),
                     'answer_content':lambda response:'\n'.join(response.css('.ui_qtext_rendered_qtext>.ui_qtext_para::text').extract()),
                     }

    asker_css_dict={'asker_name':lambda response: response.css('.feed_item_activity>span>a::text').extract_first() if response.css('.feed_item_activity>span>a::text').extract_first() else 'Anonymous',
                    'asker_url':lambda response: response.css('.feed_item_activity>span>a::attr(href)').extract_first(),
                    'question_timestamp':lambda response: (datetime.now()-quora_timedelta(response.css('.log_action_bar>.datetime::text').extract_first())).strftime('%d-%m-%Y %H:%M:%S'),
                    }

    # Assign default key to most of quora body request loads
    def default_param(self,response_text,load_text):
        for key in ['formkey', 'postkey', 'revision']: load_text[key] = self.key_token(key,response_text,self.key_pttr)
        load_text['window_id'] = self.get_window_id.search(response_text).group()
        return load_text

    # Start request from main domain www.quora.com
    def start_requests(self):
        yield scrapy.Request(url=self.main_domain,callback=self.parse)

    # Post login request
    def parse(self, response):
        # To appropriate login, you need to sign the login request with the cookie
        self.header_request.update({'Cookie': str(response.headers[b'Set-Cookie'], 'utf-8')})
        self.login_load=self.default_param(response.text,self.login_load)
        yield scrapy.Request(url=self.login_api,method='POST',callback=self.parse_question,
                             headers=self.header_request,body=urlencode(self.login_load))

    # Start to parse every question in the question jsons we scraped previously
    def parse_question(self,response):
        logging.info(response.text)
        self.header_request.update({'Cookie': str(response.headers[b'Set-Cookie'], 'utf-8')})

        for item in self.SQLITE['QUESTIONS'].select_all():
            if self.SQLITE['ASKERS'].select(list_label=['qid'],des_label='qid',source_value=item['qid']):continue
            yield scrapy.Request(url=self.main_domain+item['question_url']+'/log',callback=self.parse_answer,
                                 headers=self.header_request,meta={'question':item['question_url']})

    # Parse answer from first page of each question log
    def parse_answer(self,response):
        # Default item contain qid and question url info
        main_item=dict(qid=self.get_qid.search(response.text).group())

        # List of all log activities, from here we will know which log is answer, which log is question added
        List_Logs=response.css(self.logs_css).extract()

        # Begin to filter each log into answer, question add info and nothing
        for log in List_Logs:
            html_selector=Selector(text=log)
            item = main_item.copy()
            # Answer url is signal that the log activity we are parsing is answer, if not we will test
            # if it is question added info by checking the quote "question added by" in inner html of the log
            if not self.answer_css_dict['answer_url'](html_selector):

                try:
                    check_question_added = 'question added by' in html_selector.css(self.question_add_check).extract_first().lower()
                except:
                    continue
                # if the log is question added info, save it to table asker
                if check_question_added:
                    item.update({key: css(html_selector) for key, css in self.asker_css_dict.items()})
                    yield {'table':'asker','record':item}
                continue

            # if we can be here, the log is answer, we will parse general info of answer before
            # access to detail page of answer to parse for its full content
            item.update({key: css(html_selector) for key, css in self.answer_css_dict.items()})
            # if answer has already been scraped, move on
            if self.SQLITE['ANSWERS'].select(list_label=['answer_id'],des_label='answer_id',source_value=item['answer_id']):continue

            yield scrapy.Request(url=self.main_domain + item['answer_url'], callback=self.parse_detail,
                                 headers=self.header_request, meta={'item': item.copy()})

        # check if there is next page, if yes, let's fetch some ajax request to call next page
        # if response.css(self.next_page_check).extract_first():
        # important parameter to parse for next page, take referrence to next_log_load to understand these parameter
        try:
            current_sum=self.init_const
            cid=self.get_cid.search(response.text).group()
            hash=self.get_log_hash.search(response.text).group()
            channel=self.get_log_channel.search(response.text).group()
            chan=self.get_log_chan.search(response.text).group()
            next_log_json=self.next_log_json % (cid,current_sum)
            next_log_load = self.default_param(response.text, self.next_log_load)
            next_log_load['json']=next_log_json
            yield scrapy.Request(url=self.next_log_api,callback=self.parse_next,headers=self.header_request,body=urlencode(next_log_load),method='POST',
                                 meta={'cid': cid, 'hash': hash, 'channel': channel, 'chan': chan,'current_sum': current_sum,'min_seq':0,'main_item':main_item,'next_log_load':next_log_load.copy()})
        except:pass


    # once we fetch request for the next page, we need to access the next log url with the right min seq
    # to get the page we have just call for. remember the min seq from response of this request, which is used to access next page if available.
    def parse_next(self,response):
        yield scrapy.Request(url=self.next_log_url % (response.meta['chan'],response.meta['min_seq'],response.meta['channel'],response.meta['hash']),headers=self.header_request,
                             meta={key:response.meta[key] for key in self.next_log_meta_keys},callback=self.parse_next_answer)

    # parse log activity from next page and try to reach next page of this page if available, the same as parse_answer
    def parse_next_answer(self,response):
        list_html=self.get_htmls.findall(response.text)
        if not list_html:return
        for html in list_html:
            html_selector=Selector(text=html)
            if not self.answer_css_dict['answer_url'](html_selector):

                try:
                    check_question_added='question added by' in html_selector.css(self.question_add_check).extract_first().lower()
                except:
                    continue

                if check_question_added:
                    item=response.meta['main_item']
                    item.update({key: css(html_selector) for key, css in self.asker_css_dict.items()})
                    yield {'table':'asker','record':item}
                continue
            item=response.meta['main_item'].copy()
            item.update({key: css(html_selector) for key, css in self.answer_css_dict.items()})

            # if answer has already been scraped, move on
            if self.SQLITE['ANSWERS'].select(list_label=['answer_id'], des_label='answer_id',
                                             source_value=item['answer_id']): continue

            yield scrapy.Request(url=self.main_domain + item['answer_url'], callback=self.parse_detail,
                                 headers=self.header_request, meta={'item': item.copy()})


        if len(list_html)<self.inc_const:return
        next_meta={key:response.meta[key] for key in self.next_log_meta_keys}
        next_meta.update({'current_sum':next_meta['current_sum']+self.inc_const,
                          'min_seq':self.get_min_seq.search(response.text).group()})
        next_log_load=response.meta['next_log_load']
        next_log_load.update({'json':self.next_log_json % (response.meta['cid'],next_meta['current_sum'])})

        yield scrapy.Request(url=self.next_log_api, callback=self.parse_next, headers=self.header_request,
                             body=urlencode(next_log_load), method='POST',meta=next_meta)

    # parse for full content of answer before send them to answer table
    def parse_detail(self,response):
        item=response.meta['item']
        item.update({key:css(response) for key,css in self.detail_css_dict.items()})
        yield {'table':'answer','record':item}

