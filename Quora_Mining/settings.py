# -*- coding: utf-8 -*-

# Scrapy settings for Quora_Mining project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'Quora_Mining'

SPIDER_MODULES = ['Quora_Mining.spiders']
NEWSPIDER_MODULE = 'Quora_Mining.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'

LOGIN_COOKIES=[
    {'m-b':r"iBseHETIeWDk-udIIu9umw\075\075"},
    {'m-b':r"OsbhFpeTOCIIPgdSqh6T9g\075\075"},
    {'m-b':r"ryUIPeZE6G5VHVs6ls7s2A\075\075"},
    {'m-b':r"d9GTkUZC6sJi7a-SwbuTSQ\075\075"},
    {'m-b':r"lsEHdJIjChgjOEeLqnBUXw\075\075"},
    {'m-b':r"GuCE5CGZLNGa78aUzkUeBw\075\075"},
    {'m-b':r"sFuZtp5UmLuCmc2l6WJ4Vw\075\075"},
    {'m-b':r"l-ELc8IykONs03_tBLbf_A\075\075"},
    {'m-b':r"e4YTAVoZVCp-YVyOnGV2bA\075\075"},
    {'m-b':r"nX7acIDuebM058qew7_6JQ\075\075"},
    {'m-b':r"7joeT_N4wnvAXerxrTSleA\075\075"},
    {'m-b':r"pmpJOr1uqU3f1ip36JGjYQ\075\075"},
    {'m-b':r"N4_aMiO8-cGDPck4YSvxVA\075\075"},
    {'m-b':r"1-ul2UIT13o0Dy-VHEWlFw\075\075"},
    {'m-b':r"OsbhFpeTOCIIPgdSqh6T9g\075\075"},
]


DB_PATH='quora.db'
QUESTIONS='quora_question'
ANSWERS='quora_answers'
ASKERS='quora_askers'
USERS='quora_users'
FOLLOWERS='quora_followers'
FOLLOWING='quora_following'
ACTIVITIES='quora_activities'

SCAN_DURATION=30
SAVE_CHUNK=100

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY =.4
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 10
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'Quora_Mining.middlewares.QuoraMiningSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#     'scrapy_crawlera.CrawleraMiddleware': 610,
# }
#
# CRAWLERA_ENABLED = True
# CRAWLERA_APIKEY = '6e587ddfaae6446cb6c19823e0827fd4'


# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'Quora_Mining.pipelines.QuoraMiningPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
