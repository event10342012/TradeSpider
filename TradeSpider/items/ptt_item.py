from scrapy import Item, Field


class TitleItem(Item):
    id = Field()
    title_name = Field()
    push_num = Field()
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
