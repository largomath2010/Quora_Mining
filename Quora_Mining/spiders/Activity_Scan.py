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
    name = 'Activity_Scan'

    # Question_file
    main_domain=r'https://www.quora.com'
    next_follow_api = {'update':r"https://www.quora.com/webnode2/server_call_POST?_h=2jeqFNULNIPABB&_m=update_list",
                       'increase':r'https://www.quora.com/webnode2/server_call_POST?_h=2jeqFNULNIPABB&_m=increase_count'}
    next_follow_url = r'https://tch.tch.quora.com/up/%s/updates?&min_seq=%s&channel=%s&hash=%s&timeout=2000'

    # Request parameters
    next_follow_js = '{"hashes":%s,"has_more":true,"extra_data":{},"serialized_component":"%s","not_auto_paged":false,"auto_paged":true,"enable_mobile_hide_content":false,"auto_paged_offset":800,"loading_text":"Loading...","error_text":"Connection+Error.+Please+refresh+the+page.","aggressively_load_2nd_page":false}'
    next_follow_json = {'update':'{"args":[],"kwargs":{"paged_list_parent_cid":"wZbPomFd17","filter_hashes":%s,"extra_data":{},"force_cid":"wZbPomFd34","new_page":true}}',
                        'increase':'{"args":[],"kwargs":{"cid":"%s","num":20,"current":%s}}'}

    header_request = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'}

    next_follow_load = {'update':dict(json='', revision='', formkey='', postkey='', window_id='', referring_controller='user',
                                      referring_action='log', __hmac='2jeqFNULNIPABB', __method='update_list',
                                      js_init='', __metadata='{}'),
                        'increase':dict(json='', revision='', formkey='', postkey='', window_id='', referring_controller='user',
                                        referring_action='log', __hmac='2jeqFNULNIPABB', __method='increase_count',
                                        js_init='{"object_id":96311107,"initial_count":20,"buffer_count":20,"crawler":false,"has_more":true,"retarget_links":true,"fixed_size_paged_list":false,"auto_paged":true}')}

    # Regex tool
    key_pttr = r'(?<=\"{0}\"\:\s\")[^\"]*?(?=\")'
    key_token = lambda r,x,y,k: regex.compile(k.format(x), regex.IGNORECASE).search(y).group()
    get_window_id = regex.compile(r'(?<=window_id\s\=\s\")[^\"]*?(?=\")', regex.IGNORECASE)

    # Regex for update api
    get_filter_hash = regex.compile(r'(?<=\"hashes\"\:\s)[^\]]*?\](?=\,\s\"has\_more\"\:\strue\,\s\"extra\_data)',regex.IGNORECASE)
    get_serial = regex.compile(r'(?<=\"serialized\_component\"\:\s\")[^\"]*?(?=\"\,\s\"not\_auto\_paged)',regex.IGNORECASE)

    # Regex for increase api
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
    last_check=datetime.now()-timedelta(days=settings.get('SCAN_DURATION'))


    inc_const=20
    init_const = 20

    next_meta_keys = {'update':['hash', 'serial', 'item', 'next_follow_load','type','callback'],
                      'increase':['cid', 'hash', 'channel', 'chan','current_sum','min_seq','item','next_follow_load','type','callback']}

    custom_settings = {
        'ITEM_PIPELINES':{
            'Quora_Mining.pipelines.Save_Network':50,
        },
    }

    # List css:
    activities_css=r'.pagedlist_item'

    activities_css_dict = {'question_text':lambda response:response.css(r'[class*=ui_qtext_rendered_qtext]::text').extract_first(),
                           'question_url': lambda response:response.css(r'.question_link::attr(href)').extract_first(),
                           'activity_type':lambda response:response.css(r'.feed_item_activity::text,.feed_item_activity>*::text').extract_first(),
                           'activity_url':lambda response:response.css(r'.log_action_bar>a::attr(href)').extract_first(),
                           'id':lambda response:response.css(r'.log_action_bar>a::text').extract_first(),
                           'timestamp': lambda response: (datetime.now()-quora_timedelta(response.css(r'.datetime::text').extract_first()))}

    # Assign default key to most of quora body request loads
    def default_param(self,response_text,load_text):
        for key in ['formkey', 'postkey', 'revision']: load_text[key] = self.key_token(key,response_text,self.key_pttr)
        load_text['window_id'] = self.get_window_id.search(response_text).group()
        return load_text

    # Start to parse every answerer,asker in the answers and askers db we scraped previously
    def start_requests(self):
        self.LIST_URLS = {'USERS': [item['user_url'] for item in SQL(db_path=self.db_path, db_table=self.settings.get('USERS')).select_all() if str(item['server'])==str(self.server)],
                          'ACTIVITIES': [item['user_url'] for item in SQL(db_path=self.db_path, db_table=self.settings.get('ACTIVITIES')).select_all()]}

        for user in self.LIST_URLS['USERS']:
            if not user or user=='None':continue
            if user in self.LIST_URLS['ACTIVITIES'] and self.LIST_URLS['ACTIVITIES'].count(user)<self.init_const:
                logging.info(user + ' Has already been processed!')
                continue
            yield scrapy.Request(url=self.main_domain+user+'/log',callback=self.parse_activities,
                                 headers=self.header_request,meta={'user_url':user},cookies=self.login_cookies[int(self.server)-1])

    # Parse topics from the first page
    def parse_activities(self,response):

        try:
            meta={'type':'update',
                  'callback':self.parse_next_activities_update,
                  'hash': json.loads(self.get_filter_hash.search(response.text).group()),
                  'serial': self.get_serial.search(response.text).group(),
                  'next_follow_load': self.default_param(response.text, self.next_follow_load['update'].copy())}

            meta['next_follow_load'].update({'json': self.next_follow_json['update'] % json.dumps(meta['hash']),
                                             'js_init': self.next_follow_js % (json.dumps(meta['hash']), meta['serial'])})

        except Exception as Err:

            try:
                meta = {'type':'increase',
                        'callback':self.parse_next_append_activities,
                        'current_sum': self.init_const,
                        'cid': self.get_cid.search(response.text).group(),
                        'hash': self.get_hash.search(response.text).group(),
                        'channel': self.get_channel.search(response.text).group(),
                        'chan': self.get_chan.search(response.text).group(),
                        'min_seq': 0,
                        'next_follow_load': self.default_param(response.text, self.next_follow_load['increase'].copy())}

                meta['next_follow_load'].update({'json': self.next_follow_json['increase'] % (meta['cid'], meta['current_sum'])})
            except:
                pass

        finally:
            try:
                meta['item']={'user_url': response.meta['user_url']}
            except:
                meta={'item': {'user_url': response.meta['user_url']}}

            yield from self.append_activities(response, meta)

    # Parse topics from pagination url
    def parse_next_activities_update(self,response):
        json_data=json.loads(response.text)

        try:
            html_selector = Selector(text=json_data['value']['html'])
        except:
            return

        meta = {key: response.meta[key] for key in self.next_meta_keys[response.meta['type']]}

        meta['hash']=meta['hash']+json_data['value']['hashes']

        meta['next_follow_load'].update({'json': self.next_follow_json[response.meta['type']] % json.dumps(meta['hash']),
                                         'js_init':self.next_follow_js % (json.dumps(meta['hash']),meta['serial'])})

        yield from self.append_activities(html_selector,meta)

    def parse_next_append_activities(self, response):

        chan = response.meta['chan']
        min_seq = response.meta['min_seq']
        channel = response.meta['channel']
        hash = response.meta['hash']

        yield scrapy.Request(url=self.next_follow_url % (chan, min_seq, channel, hash), headers=self.header_request,
                             meta={key: response.meta[key] for key in self.next_meta_keys[response.meta['type']]}.copy(),
                             callback=self.parse_next_activities_increase)

    def parse_next_activities_increase(self,response):
        list_html = self.get_htmls.findall(response.text)

        if not list_html:
            return

        html_selector = Selector(text='\n'.join(list_html))
        meta = {key: response.meta[key] for key in self.next_meta_keys[response.meta['type']]}
        meta.update({'current_sum': meta['current_sum'] + self.inc_const,
                     'min_seq': self.get_min_seq.search(response.text).group()})
        meta['next_follow_load'].update({'json': self.next_follow_json[response.meta['type']] % (meta['cid'], meta['current_sum'])})

        yield from self.append_activities(html_selector, meta)

    # Common procedure that parse topics and yield pagination happening in both first page of topics and pagination page
    def append_activities(self,html_selector,meta):
        list_activities=html_selector.css(self.activities_css).extract()
        inbound_timestamp=[]

        for activity in list_activities:
            item=meta['item'].copy()
            item.update({key:funct(Selector(text=activity)) for key,funct in self.activities_css_dict.items()})
            if item['timestamp']>=self.last_check:
                inbound_timestamp.append(item['timestamp'])
                item['timestamp']=item['timestamp'].strftime('%Y-%m-%d')
                yield item

        if not inbound_timestamp:
            return

        if len(list_activities) == self.init_const:
            if 'next_follow_load' not in meta.keys():
                logging.info('No next page!')
                return

            yield scrapy.Request(url=self.next_follow_api[meta['type']], callback=meta['callback'], headers=self.header_request,
                                 body=urlencode(meta['next_follow_load']), method='POST', meta=meta.copy())
        else:
            logging.info('No next page!')



