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
    name = 'User_Scan'

    # Question_file
    main_domain=r'https://www.quora.com'
    next_topic_api=r"https://www.quora.com/webnode2/server_call_POST?_h=jhFnvm%2FmmJ1Fza&_m=increase_count"
    next_topic_url=r'https://tch.tch.quora.com/up/%s/updates?&min_seq=%s&channel=%s&hash=%s&timeout=2000'


    # Request parameters
    next_topic_json = '{"args":[],"kwargs":{"cid":"%s","num":18,"current":%s}}'

    header_request={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'}

    next_topic_load = dict(json='', revision='', formkey='', postkey='', window_id='', referring_controller='user',
                         referring_action='topic', __hmac='jhFnvm/mmJ1Fza', __method='increase_count',
                         js_init='{"object_id":25595509,"initial_count":18,"buffer_count":18,"crawler":false,"has_more":true,"retarget_links":true,"fixed_size_paged_list":false,"auto_paged":true}')

    # Regex tool
    key_pttr = r'(?<=\"{0}\"\:\s\")[^\"]*?(?=\")'
    key_token = lambda r,x,y,k: regex.compile(k.format(x), regex.IGNORECASE).search(y).group()
    get_window_id = regex.compile(r'(?<=window_id\s\=\s\")[^\"]*?(?=\")', regex.IGNORECASE)

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
    login_cookies=settings.get('LOGIN_COOKIES')
    SQLITE={'QUESTIONS':SQL(db_path=db_path, db_table=questions),
            'ANSWERS':SQL(db_path=db_path, db_table=answers),
            'ASKERS':SQL(db_path=db_path, db_table=askers)}

    inc_const = 18
    init_const = 18
    next_topic_meta_keys=['cid', 'hash', 'channel', 'chan','current_sum','min_seq','item','next_topic_load']

    # custom_settings = {
    #     'ITEM_PIPELINES':{
    #         'Quora_Mining.pipelines.Answers_Or_Askers':50,
    #     },
    # }


    # List css:
    topics_css=r'div.ObjectCard-header>div>a>span.name_text>span>.TopicName::text'

    user_css_dict = {'user':lambda response:response.css(r'.ProfileNameAndSig>h1>span>span>span::text').extract_first(),
                     'questions': lambda response:response.css(r'.QuestionsNavItem>a>span::text').extract_first(),
                     'answers': lambda response: response.css(r'.AnswersNavItem>a>span::text').extract_first(),
                     'posts': lambda response: response.css(r'.PostsNavItem>a>span::text').extract_first(),
                     'blogs': lambda response: response.css(r'.BlogsNavItem>a>span::text').extract_first(),
                     'shares':lambda response:response.css(r'.QuoraShareNavItem >a>span::text').extract_first()}

    # Assign default key to most of quora body request loads
    def default_param(self,response_text,load_text):
        for key in ['formkey', 'postkey', 'revision']: load_text[key] = self.key_token(key,response_text,self.key_pttr)
        load_text['window_id'] = self.get_window_id.search(response_text).group()
        return load_text

    # Start to parse every question in the question jsons we scraped previously
    def start_requests(self):
        for item in self.SQLITE['ANSWERS'].select_all():
            # if self.SQLITE['USERS'].select(list_label=['user_url'],des_label='user_url',source_value=item['answerer_url']):continue
            yield scrapy.Request(url=self.main_domain+item['answerer_url'],callback=self.parse_user,
                                 headers=self.header_request,meta={'user_url':item['answerer_url'],'cookies':self.login_cookies})

    # Parse answer from first page of each question log
    def parse_user(self,response):
        # Default item contain user url and id info
        item=dict(user_url=response.meta['user_url'])
        item.update({key:funct(response) for key,funct in self.user_css_dict.items()})
        yield scrapy.Request(url=response.request.url+'/topics',headers=self.header_request,
                             callback=self.parse_topics,meta={'item':item.copy()})

    def parse_topics(self,response):
        item=response.meta['item']

        initial_meta={'current_sum':self.init_const,
                      'cid':self.get_cid.search(response.text).group(),
                      'hash':self.get_log_hash.search(response.text).group(),
                      'channel':self.get_log_channel.search(response.text).group(),
                      'chan':self.get_log_chan.search(response.text).group(),
                      'min_seq':0,
                      'item':item.copy()}
        next_topic_load = self.default_param(response.text, self.next_log_load)
        next_topic_load['json'] = self.next_topic_json % (meta['cid'], meta['current_sum'])
        initial_meta.update({'next_topic_load':next_topic_load})

        item = self.append_topics(response,item,initial_meta)

        yield scrapy.Request(url=self.next_log_api, callback=self.parse_next_topics, headers=self.header_request,
                             body=urlencode(next_topic_load), method='POST',meta=initial_meta)

    def parse_next_topics(self,response):

        chan=response.meta['chan']
        min_seq=response.meta['min_seq']
        channel=response.meta['channel']
        hash=response.meta['hash']

        yield scrapy.Request(url=self.next_topic_url % (chan,min_seq,channel,hash),headers=self.header_request,
                             meta={key: response.meta[key] for key in self.next_topic_meta_keys},callback=self.parse_next_topics_detail)

    def parse_next_topics_detail(self,response):
        list_html = self.get_htmls.findall(response.text)
        item=response.meta['item']

        if not list_html:
            yield item
        else:
            html_selector = Selector(text='\n'.join(list_html))
            self.parse_topics(html_selector)

    def append_topics(self,html_selector,item,meta):

        try:
            list_topics = item['topics']
        except:
            list_topics=[]

        item['topics'] = list_topics + html_selector.css(self.topics_css).extract()

        next_meta = {key: response.meta[key] for key in self.next_log_meta_keys}
        next_meta.update({'current_sum': next_meta['current_sum'] + self.inc_const,
                          'min_seq': self.get_min_seq.search(response.text).group()})
        next_log_load = response.meta['next_log_load']
        next_log_load.update({'json': self.next_log_json % (response.meta['cid'], next_meta['current_sum'])})

        yield scrapy.Request(url=self.next_log_api, callback=self.parse_next_topics, headers=self.header_request,
                             body=urlencode(next_topic_load), method='POST', meta=meta)



