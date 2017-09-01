import scrapy


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

    if 'imprimir' in url:
        return False

    if 'form-id' in url:
        return False

    if 'external-site' in url:
        return False

    if 'filter' in url:
        return False
    return True


class CfgovSpider(scrapy.Spider):
    name = "cfgov"
    handle_httpstatus_list = [404, 301, 302, 500]
    allowed_domains = ['www.consumerfinance.gov',
                       'files.consumerdinance.gov',
                       's3.amazonaws.com'
                       ]

    def start_requests(self):
        urls = [
                'https://www.consumerfinance.gov/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        next = response.headers.get('Location')
        yield Result(url=response.url, status=response.status, next=next)

        if response.status in [301, 302]:
            yield scrapy.Request(url=next)
            return

        if response.status >= 400:
            return

        try:
            links = response.css('a::attr(href)')
        except scrapy.exceptions.NotSupported:
            # This just means that the response isn't HTML
            return

        viable_links = filter(filter_selector, links)
        for link in viable_links:
            destination = response.urljoin(link.extract().strip())
            destination = destination.replace(':443','')
            yield Link(source=response.url, destination=destination)
            yield scrapy.Request(url=destination)
