BOT_NAME = "autoria_parser"

SPIDER_MODULES = ["autoria_parser.spiders"]
NEWSPIDER_MODULE = "autoria_parser.spiders"

ROBOTSTXT_OBEY = True

ITEM_PIPELINES = {
    #    "autoria_parser.pipelines.AutoriaParserPipeline": 300,
    "autoria_parser.pipelines.AutoriaParserNoDuplicatesPipeline": 300
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
