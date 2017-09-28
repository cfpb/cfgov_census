import scrapy
from urlnorm import norm, InvalidUrl
from scrapy.utils.url import canonicalize_url, strip_url


def clean_url(url):
    url = strip_url(url)
    url = url.replace('///','//')
    url = canonicalize_url(url)
    url = norm(url)
    url = url.strip() # should be unneccessary?
    if '..' in url:
        url = url.replace('../', '')
    return url

class Link(scrapy.Item):
    source = scrapy.Field()
    destination = scrapy.Field()


class Result(scrapy.Item):
    url = scrapy.Field()
    status = scrapy.Field()
    next = scrapy.Field()


def filter_selector(selector):
    url = selector.extract()

    if url.startswith('#'):
        return False

    if 'external-site' in url:
        return False

    if 'mailto:' in url:
        return False

    if 'feed:' in url:
        return False

    if 'tel:' in url:
        return False

    return True


class CfgovSpider(scrapy.Spider):
    name = "cfgov"
    handle_httpstatus_list = [404, 301, 302, 500]
    allowed_domains = ['www.consumerfinance.gov',
                       'files.consumerfinance.gov',
                       's3.amazonaws.com',
                       ]

    def start_requests(self):
        urls = [
                'https://www.consumerfinance.gov/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        next = response.headers.get('Location')
        yield Result(url=clean_url(response.url), status=response.status, next=next)

        if response.status in [301, 302]:
            yield scrapy.Request(url=next)
            return

        if response.status >= 400:
            return

        try:
            # try looking for links in '<main> tags first-- that should omit
            # header and footer links. Otherwise, fall back to every link
            links = response.css('a::attr(href)')
        except scrapy.exceptions.NotSupported:
            # This just means that the response isn't HTML
            return

        viable_links = set(filter(filter_selector, links))
        processed_links = []
        for link in viable_links:
            try:
                destination = clean_url(response.urljoin(link.extract().strip()))
            except InvalidUrl:
                continue

            if destination not in processed_links:
                yield Link(source=response.url, destination=destination)
                yield scrapy.Request(url=destination)
                processed_links.append(destination)
