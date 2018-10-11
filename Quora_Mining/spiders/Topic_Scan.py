# -*- coding: utf-8 -*-
import scrapy,regex,json,logging
from scrapy.selector import Selector
from urllib.parse import urlencode
from scrapy.utils.project import get_project_settings
from Quora_Mining.SQL import SQL
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

class TopicScanSpider(scrapy.Spider):
    name = 'Topic_Scan'

    # Request url and api
    main_domain='https://www.quora.com'
    main_url = 'https://www.quora.com/topic/Technology/all_questions'
    login_api = r"https://www.quora.com/webnode2/server_call_POST?_h=Vn03YsuKFZvHV9&_m=do_login"
    more_topic_api=r"https://www.quora.com/webnode2/server_call_POST?_h=UylPNrkrE2pj93&_m=fetch_toggled_component"
    next_log_api=r"https://www.quora.com/webnode2/server_call_POST?_h=Cm4yo/TgmQFYUW&_m=increase_count"
    next_log_url = r"https://tch.tch.quora.com/up/%s/updates?&min_seq=%s&channel=%s&hash=%s&timeout=2000"

    # Request parameters
    user='muoinguyen.idgvv@gmail.com'
    password='UnitedKingdom'
    login_json = '{"args":[],"kwargs":{"email":"%s","password":"%s"}}'
    more_topic_json = '{"args":[],"kwargs":{"serialized_args":"%s"}}'
    next_log_json = '{"args":[],"kwargs":{"cid":"%s","num":20,"current":%s}}'
    next_init_json='{"object_id":%s,"initial_count":20,"buffer_count":20,"crawler":false,"has_more":true,"retarget_links":true,"fixed_size_paged_list":false,"auto_paged":true}'

    header_request={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'}
    login_load={'json':login_json % (user,password), 'formkey': '', 'postkey': '', 'revision': '', 'window_id': '', '__method': 'do_login',
                '__hmac': 'Vn03YsuKFZvHV9', 'referring_action': 'index', 'referring_controller': 'index', '__metadata': {}, 'js_init': {}}

    more_topic_load=dict(json='',revision='',formkey='',postkey='',window_id='',referring_controller='question',referring_action='q',
                         parent_cid='',parent_domid='',domids_to_remove=[],__hmac='UylPNrkrE2pj93',__method='fetch_toggled_component',__metadata={})
    next_log_load = dict(json='', revision='', formkey='', postkey='', window_id='', referring_controller='topic',
                         referring_action='all_questions', __hmac='Cm4yo/TgmQFYUW', __method='increase_count',__first_server_call='true',__metadata={},
                         js_init='{"object_id":3906,"initial_count":20,"buffer_count":20,"crawler":false,"has_more":true,"retarget_links":true,"fixed_size_paged_list":false,"auto_paged":true}')

    # Regex tool
    key_pttr = r'(?<=\"{0}\"\:\s\")[^\"]*?(?=\")'
    key_token = lambda r,x,y,k: regex.compile(k.format(x), regex.IGNORECASE).search(y).group()
    get_window_id = regex.compile(r'(?<=window_id\s\=\s\")[^\"]*?(?=\")', regex.IGNORECASE)
    get_more_topic_serializer = regex.compile(r"(?<=\"fetch\_into\"\:\s\"\@fetch\_into\".*?\"serialized\_args\"\:\s\")[^\"]*?(?=\")", regex.IGNORECASE)
    get_more_topic_cid=regex.compile(r'(?<=\"ToggleBase\"\,\s\").*?(?=\"\,\s\"\"\,\s\{\"fetch\_on\"\:\s\[\"click\"\,\s\"\@more)',regex.IGNORECASE)
    get_more_topic_domid=lambda self,response,cid:json.loads(regex.compile(r"(?<=\"domids\"\:\s)\{.*?\}",regex.IGNORECASE).search(response).group())[cid]

    get_cid = regex.compile(r'(?<=\"PagedList\"\,\s\")[^"]*?(?=\"\,\s\"\"\,\s\{\"object_id\"\:\s\d)', regex.IGNORECASE)
    get_htmls = regex.compile(r'(?<=\\"html\\"\:\s\\").*?(?=\\"\,\s\\"css\\")', regex.IGNORECASE)
    get_min_seq = regex.compile(r'(?<=\"min\_seq\"\:).*?(?=\})', regex.IGNORECASE)

    get_log_chan = regex.compile(r'(?<=\")chan[^\"]*?(?=\"\,\s\"quora\.com)', regex.IGNORECASE)
    get_log_hash = regex.compile(r'(?<=")[^\"]*?(?=\"\,\s\"chan)', regex.IGNORECASE)
    get_log_channel = regex.compile(r'(?<=start\(0\,\s\")[^\"]*?(?=\")', regex.IGNORECASE)

    # Other tools
    settings = get_project_settings()
    processed_topics={}
    SQLITE=SQL(db_path=settings.get('DB_PATH'),db_table=settings.get('QUESTIONS'))
    last_milestone=datetime.now()-timedelta(days=settings.get('SCAN_DURATION'))
    inc_const = 20
    init_const = 20

    next_log_meta_keys = ['cid', 'hash', 'channel', 'chan', 'current_sum', 'min_seq', 'main_item', 'next_log_load']

    custom_settings = {
        'ITEM_PIPELINES':{
            'Quora_Mining.pipelines.Question_To_Csv':50,
        },
    }

    # List css:
    related_topic_css=r"div[id]>span[id].TopicName::text"
    related_topic_link_css=r"div[id]>a[id].HoverMenu.RelatedTopicsListItem::attr(href)"
    topic_follow_css=r".TopicPageHeader>div>div.content_wrapper>div>div[id]>div>div>span>[action_click*=Topic]>.icon_action_bar-label>span>span[id]::text"
    current_topic_css=r"h1>span[id]>span[id].TopicName::text"
    more_topic_check=r":not(.hidden)>.view_more_topics_link"
    more_topic_name_css=r".TopicName::text"
    question_htmls_css=r'.pagedlist_item>.feed_item'
    next_page_check = r'.pager_next.action_button:not(.hidden)'
    tid_css=r'.FollowPrimaryActionItem>span>a::attr(action_target)'

    topics_css_dict={'question_text':lambda response:response.css(r'[class*=ui_qtext_rendered_qtext]::text').extract_first(),
                     'question_url':lambda response:response.css(r'.question_link::attr(href)').extract_first(),
                     'question_follow':lambda response:response.css(r"[action_click*=Question]>div>span>span[id]::text").extract_first(),
                     'qid':lambda response:json.loads(response.css(r'[action_target*=qid]::attr(action_target)').extract_first())['qid'],
                     'last_active':lambda response:datetime.now()-quora_timedelta(response.css(r".datetime::text").extract_first()),
                     }

    next_css_dict={'question_url':lambda response:response.css(r'.question_link::attr(href)').extract_first(),
                   'last_active':lambda response:datetime.now()-quora_timedelta(response.css(r".datetime::text").extract_first()),
                   }

    question_css_dict={'last_ask':lambda response:(datetime.now()-quora_timedelta(response.css(r'.AskedRow>span[id]::text').extract_first())).strftime('%d-%m-%Y %H:%M:%S'),
                       'views':lambda response:response.css(r'.ViewsRow::text').extract_first(),
                       'topics':lambda response:','.join(response.css('.TopicNameSpan.TopicName::text').extract()),
                       'question_text':lambda response:response.css(r'[class*=ui_qtext_rendered_qtext]::text').extract_first(),
                       'question_follow': lambda response: response.css(r"[action_click*=Question]>div>span>span[id]::text").extract_first(),
                       'qid': lambda response:json.loads(response.css(r'[action_target*=qid]::attr(action_target)').extract_first())['qid'],
                       }

    # Default procedure that applying to all body request of all type of quora pagination
    def default_param(self,response_text,load_text):
        for key in ['formkey', 'postkey', 'revision']: load_text[key] = self.key_token(key,response_text,self.key_pttr)
        load_text['window_id'] = self.get_window_id.search(response_text).group()
        return load_text

    # Get started with technology topic
    def start_requests(self):
        yield scrapy.Request(url=self.main_domain,callback=self.parse)

    # Get login
    def parse(self, response):
        self.header_request.update({'Cookie': str(response.headers[b'Set-Cookie'], 'utf-8')})
        self.login_load=self.default_param(response.text,self.login_load)
        yield scrapy.Request(url=self.login_api,method='POST',callback=self.parse_postlog,
                             headers=self.header_request,body=urlencode(self.login_load))

    # Get to technology topic after loging in
    def parse_postlog(self,response):
        self.header_request.update({'Cookie': str(response.headers[b'Set-Cookie'], 'utf-8')})
        yield scrapy.Request(url=self.main_url,callback=self.parse_topic,headers=self.header_request)

    # Parse the first page of topic
    def parse_topic(self,response):
        self.header_request.update({'Cookie': str(response.headers[b'Set-Cookie'], 'utf-8')})

        # Parse related topic
        for pair in zip(response.css(self.related_topic_css).extract(),response.css(self.related_topic_link_css).extract()):
            # if related topic has already been parsed, then move on to the next
            if pair[0] in self.processed_topics.keys():continue
            self.processed_topics[pair[0]]=self.main_domain+pair[1]+"/all_questions"
            yield scrapy.Request(url=self.processed_topics[pair[0]],callback=self.parse_topic,headers=self.header_request)

        current_topic=response.css(self.current_topic_css).extract_first()

        if current_topic not in self.processed_topics.keys():
            self.processed_topics[current_topic]=response.request.url

        main_item=dict(origin_topic_url=response.request.url,
                       topic_follow=response.css(self.topic_follow_css).extract_first(),
                       timestamp=datetime.now().strftime('%d-%m-%Y %H:%M:%S'))

        List_Questions=response.css(self.question_htmls_css).extract()

        # start parse each question from first page
        for Question in List_Questions:
            item=main_item.copy()
            item.update({key:value(Selector(text=Question)) for key,value in self.topics_css_dict.items()})
            item['last_active'] = item['last_active'].strftime('%d-%m-%Y %H:%M:%S')

            # if question already in sql then moving on
            if self.SQLITE.select(list_label=['qid'],des_label='qid',source_value=item['qid']):continue

            yield scrapy.Request(url=self.main_domain+item['question_url'],callback=self.parse_question,headers=self.header_request,meta={'item':item.copy()})

        # parse next page of current topic
        if response.css(self.next_page_check).extract_first():
            # important parameter to parse for next page, take referrence to next_log_load to understand these parameter
            current_sum = self.init_const
            cid = self.get_cid.search(response.text).group()
            hash = self.get_log_hash.search(response.text).group()
            channel = self.get_log_channel.search(response.text).group()
            chan = self.get_log_chan.search(response.text).group()
            next_log_json = self.next_log_json % (cid, current_sum)
            next_log_load = self.default_param(response.text, self.next_log_load)
            next_log_load['json'] = next_log_json
            next_log_load['js_init']=self.next_init_json % (json.loads(response.css(self.tid_css).extract_first())['tid'])
            yield scrapy.Request(url=self.next_log_api, callback=self.parse_next, headers=self.header_request,
                                 body=urlencode(next_log_load), method='POST',meta={'cid': cid, 'hash': hash, 'channel': channel, 'chan': chan,'current_sum': current_sum,
                                                                                    'min_seq': 0, 'main_item': main_item,'next_log_load': next_log_load.copy()})

    # get to the next page data after post command to server
    def parse_next(self,response):
        yield scrapy.Request(url=self.next_log_url % (response.meta['chan'],response.meta['min_seq'],response.meta['channel'],response.meta['hash']),headers=self.header_request,
                             meta={key:response.meta[key] for key in self.next_log_meta_keys},callback=self.parse_next_topic)

    # parse the data of next page of topic
    def parse_next_topic(self, response):
        # in this data, each question would be wraped in an html tag, we will parse question once by once
        list_html = self.get_htmls.findall(response.text)
        if not list_html: return
        list_timestamp=[]
        for html in list_html:
            item = response.meta['main_item'].copy()
            item.update({key: css(Selector(text=html)) for key, css in self.next_css_dict.items()})

            if item['last_active']>=self.last_milestone:
                list_timestamp.append(item['question_url'])
            item['last_active']=item['last_active'].strftime('%d-%m-%Y %H:%M:%S')

            if self.SQLITE.select(list_label=['question_url'], des_label='question_url', source_value=item['question_url']): continue
            yield scrapy.Request(url=self.main_domain + item['question_url'], callback=self.parse_question,
                                 headers=self.header_request, meta={'item': item.copy()})
        # list timestamp to store all question with last active date happened before our last milestone within current pagination. if there is no, return
        if not list_timestamp:return
        # if number of question less than maximum question per page, return (because we have gone to the end)
        if len(list_html) < self.inc_const: return

        # assign important parameter of next pagination post request
        next_meta = {key: response.meta[key] for key in self.next_log_meta_keys}
        next_meta.update({'current_sum': next_meta['current_sum'] + self.inc_const,
                          'min_seq': self.get_min_seq.search(response.text).group()})
        next_log_load = response.meta['next_log_load']
        next_log_load.update({'json': self.next_log_json % (response.meta['cid'], next_meta['current_sum'])})

        yield scrapy.Request(url=self.next_log_api, callback=self.parse_next, headers=self.header_request,
                             body=urlencode(next_log_load), method='POST', meta=next_meta)

    # parse detail info of question
    def parse_question(self,response):
        item=response.meta['item']
        item.update({key:css(response) for key,css in self.question_css_dict.items()})
        # if there are hidden topic, this would parse the hell out of them
        if response.css(self.more_topic_check).extract_first():
            more_topic_load=self.more_topic_load.copy()
            more_topic_load=self.default_param(response.text,more_topic_load)
            more_topic_load['json']=self.more_topic_json % self.get_more_topic_serializer.search(response.text).group()
            more_topic_load['parent_cid'] = self.get_more_topic_cid.search(response.text).group()
            more_topic_load['parent_domid'] = self.get_more_topic_domid(response.text, more_topic_load['parent_cid'])
            yield scrapy.Request(url=self.more_topic_api,method='POST',callback=self.parse_more_topics,
                                 headers=self.header_request,body=urlencode(more_topic_load),meta={'item':item.copy()})
        else:
            yield item

    # parse hidden topic of a question
    def parse_more_topics(self,response):
        logging.warn('FOUND A ADDED TOPICS')
        json_response=json.loads(response.text)
        html_selector=Selector(text=json_response['value']['html'])
        item=response.meta['item']
        item['topics']=','.join([item['topics']]+list(html_selector.css(self.more_topic_name_css).extract()))
        yield item






