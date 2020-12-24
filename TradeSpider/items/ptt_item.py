from scrapy import Item, Field


class PttItem(Item):
    title = Field()
    author = Field()
    date = Field()
    url = Field()
    content = Field()


class PushItem(Item):
    push_id = Field()
    author = Field()
    text = Field()
    date = Field()
    time = Field()
