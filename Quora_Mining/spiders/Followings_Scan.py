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
    name = 'Following_Scan'

    # Question_file
    main_domain=r'https://www.quora.com'
    next_follow_api=r"https://www.quora.com/webnode2/server_call_POST?_h=h6ZkAQHNIyPp/O&_m=increase_count"
    next_follow_url=r'https://tch.tch.quora.com/up/%s/updates?&min_seq=%s&channel=%s&hash=%s&timeout=2000'


    # Request parameters
    next_follow_json = '{"args":[],"kwargs":{"cid":"%s","num":18,"current":%s}}'

    header_request={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'}

    next_follow_load = dict(json='', revision='', formkey='', postkey='', window_id='', referring_controller='user',
                         referring_action='following', __hmac='h6ZkAQHNIyPp/O', __method='increase_count',
                         js_init='{"object_id":58741273,"initial_count":18,"buffer_count":18,"crawler":false,"has_more":true,"retarget_links":true,"fixed_size_paged_list":false,"auto_paged":true}')

    # Regex tool
    key_pttr = r'(?<=\"{0}\"\:\s\")[^\"]*?(?=\")'
    key_token = lambda r,x,y,k: regex.compile(k.format(x), regex.IGNORECASE).search(y).group()
    get_window_id = regex.compile(r'(?<=window_id\s\=\s\")[^\"]*?(?=\")', regex.IGNORECASE)

    get_cid = regex.compile(r'(?<=\")[^\"]*?(?=\"\:\s\{\"more_button\"\:)', regex.IGNORECASE)
    get_htmls = regex.compile(r'(?<=\\"html\\"\:\s\\").*?(?=\\"\,\s\\"css\\")', regex.IGNORECASE)
    get_min_seq = regex.compile(r'(?<=\"min\_seq\"\:).*?(?=\})', regex.IGNORECASE)

    get_chan = regex.compile(r'(?<=\")chan[^\"]*?(?=\"\,\s\")', regex.IGNORECASE)
    get_hash = regex.compile(r'(?<=\"channelHash\"\:\s\")[^\"]*?(?=\")', regex.IGNORECASE)
    get_channel = regex.compile(r'(?<=\"channel\"\:\s\")[^\"]*?(?=\")', regex.IGNORECASE)

    # Other tools
    settings = get_project_settings()
    db_path=settings.get('DB_PATH')
    login_cookies=settings.get('LOGIN_COOKIES')

    inc_const = 18
    init_const = 18
    next_meta_keys=['cid', 'hash', 'channel', 'chan','current_sum','min_seq','item','next_follow_load']

    custom_settings = {
        'ITEM_PIPELINES':{
            'Quora_Mining.pipelines.Save_Network':50,
        },
    }

    # List css:
    following_css=r'.ObjectCard-header>span>span>a'

    following_css_dict = {'following':lambda response:response.css(r'::text').extract_first(),
                         'following_url': lambda response:response.css(r'::attr(href)').extract_first()}

    # Assign default key to most of quora body request loads
    def default_param(self,response_text,load_text):
        for key in ['formkey', 'postkey', 'revision']: load_text[key] = self.key_token(key,response_text,self.key_pttr)
        load_text['window_id'] = self.get_window_id.search(response_text).group()
        return load_text

    # Start to parse every answerer,asker in the answers and askers db we scraped previously
    def start_requests(self):
        self.LIST_URLS = {'USERS': [item['user_url'] for item in SQL(db_path=self.db_path, db_table=self.settings.get('USERS')).select_all() if str(item['server'])==str(self.server)],
                          'FOLLOWING': [item['user_url'] for item in SQL(db_path=self.db_path, db_table=self.settings.get('FOLLOWING')).select_all()]}

        for user in self.LIST_URLS['USERS']:
            if not user or user=='None':continue
            if user not in self.LIST_URLS['FOLLOWING']:continue
            if user in self.LIST_URLS['FOLLOWING'] and self.LIST_URLS['FOLLOWING'].count(user)<self.init_const:
                logging.info(user + ' Has already been processed!')
                continue
            yield scrapy.Request(url=self.main_domain+user+'/following',callback=self.parse_followers,
                                 headers=self.header_request,meta={'user_url':user},cookies=self.login_cookies[int(self.server)-1])

    # Parse topics from the first page
    def parse_followers(self,response):
        try:
            meta={'current_sum':self.init_const,
                  'cid':self.get_cid.search(response.text).group(),
                  'hash':self.get_hash.search(response.text).group(),
                  'channel':self.get_channel.search(response.text).group(),
                  'chan':self.get_chan.search(response.text).group(),
                  'min_seq':0,
                  'item':{'user_url':response.meta['user_url']},
                  'next_follow_load':self.default_param(response.text, self.next_follow_load.copy())}
            meta['next_follow_load'].update({'json':self.next_follow_json % (meta['cid'], meta['current_sum'])})
            yield from self.append_following(response, meta)
        except :
            logging.info('No followings!')
            return

    # Reassign next url parameter before passing them to the next iteration
    def parse_next_following(self,response):

        chan=response.meta['chan']
        min_seq=response.meta['min_seq']
        channel=response.meta['channel']
        hash=response.meta['hash']

        yield scrapy.Request(url=self.next_follow_url % (chan,min_seq,channel,hash),headers=self.header_request,
                             meta={key:response.meta[key] for key in self.next_meta_keys}.copy(),callback=self.parse_next_following_detail)

    # Parse topics from pagination url
    def parse_next_following_detail(self,response):
        list_html = self.get_htmls.findall(response.text)

        if not list_html:
            return

        html_selector = Selector(text='\n'.join(list_html))
        meta = {key: response.meta[key] for key in self.next_meta_keys}
        meta.update({'current_sum': meta['current_sum'] + self.inc_const,
                     'min_seq': self.get_min_seq.search(response.text).group()})
        meta['next_follow_load'].update({'json': self.next_follow_json % (meta['cid'], meta['current_sum'])})

        yield from self.append_following(html_selector,meta)

    # Common procedure that parse topics and yield pagination happening in both first page of topics and pagination page
    def append_following(self,html_selector,meta):
        list_followings=html_selector.css(self.following_css).extract()

        for following in list_followings:
            item=meta['item'].copy()
            item.update({key:funct(Selector(text=following)) for key,funct in self.following_css_dict.items()})
            yield item

        if len(list_followings)==self.init_const:
            yield scrapy.Request(url=self.next_follow_api, callback=self.parse_next_following, headers=self.header_request,
                                 body=urlencode(meta['next_follow_load']), method='POST', meta=meta.copy())
        else:
            logging.info('No next page!')



