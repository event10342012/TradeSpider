import scrapy

from items.ptt_item import PttItem


class PttSpider(scrapy.Spider):
    name = 'ptt'
    max_pages = 5
    i = 0

    def start_requests(self):
        board = getattr(self, 'board', None)
        urls = [f'https://www.ptt.cc/bbs/{board}/index.html']
        for url in urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response: scrapy.http.Response, **kwargs):
        titles = response.xpath("//div[@class='r-ent']")
        for title in titles:
            try:
                url = title.xpath("div[@class='title']/a/@href").get()
                yield response.follow(url, callback=self.parse_content)
            except Exception:
                pass

        next_page = response.xpath("//div[@class='btn-group btn-group-paging']/a[@class='btn wide'][2]/@href").get()
        if next_page and self.i < self.max_pages:
            self.logger.info(f'follow {next_page}')
            self.i += 1
            yield response.follow(next_page, callback=self.parse)

    def parse_content(self, response: scrapy.http.Response):
        item = PttItem()
        item['content'] = response.xpath("//div[@id='main-content']/text()").get().replace('\n', '')
        meta = response.xpath("//span[@class='article-meta-value']")
        item['author'] = meta[0].xpath('text()').get()
        item['title'] = meta[2].xpath('text()').get()
        item['date'] = meta[3].xpath('text()').get()
        item['url'] = response.url
        yield item
