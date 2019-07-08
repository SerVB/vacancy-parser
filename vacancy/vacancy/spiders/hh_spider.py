# -*- coding: utf-8 -*-

from typing import Optional

import scrapy
from scrapy.exporters import JsonItemExporter
from scrapy.http.response import Response


def clean_up_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None

    return s.strip()


def clean_up_place(s: Optional[str]) -> Optional[str]:
    """Отсекает ненужное от строк вида 'Москва, Охотный Ряд, и еще 1'"""
    if s is None:
        return None

    if "," in s:
        return s[:s.find(",")].strip()
    else:
        return s.strip()


# WITH_SALARY = "https://hh.ru/search/vacancy?clusters=true&enable_snippets=true&only_with_salary=true&from=cluster_compensation"
SITE = "https://hh.ru/search/vacancy?clusters=true&employment={empl}&enable_snippets=true&industry={ind}&only_with_salary=true&specialization={spec}&from=cluster_professionalArea"

divided_start_urls = list()

ind_range = range(0, 100)
spec_range = range(0, 100)
employment = frozenset({
    "volunteer",
    "full",
    "part",
    "project",
    "probation",
})

for ind in ind_range:
    for spec in spec_range:
        for empl in employment:
            divided_start_urls.append(SITE.format(ind=ind, spec=spec, empl=empl))


class MyJsonItemExporter(JsonItemExporter):
    def __init__(self, file, **kwargs):
        super(MyJsonItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)


class QuotesSpider(scrapy.Spider):
    name = "hh"
    start_urls = divided_start_urls
    custom_settings = {
        # "DEPTH_LIMIT": 5,
        "FEED_EXPORTERS": {"json": "vacancy.spiders.hh_spider.MyJsonItemExporter"}
    }

    def parse(self, response: Response) -> None:
        vacancy_count_str = response.css(".header.HH-SearchVacancyDropClusters-Header::text").get()
        vacancy_count = int("".join(filter(lambda c: c.isdigit(), vacancy_count_str)))

        max_pages = 100
        vacancy_per_page = 20
        if vacancy_count > max_pages * vacancy_per_page:
            raise Exception(response.url, vacancy_count)

        for vacancy in response.css(".vacancy-serp-item"):
            yield {
                "title": clean_up_text(vacancy.css(".resume-search-item__name a::text").get()),
                "salary": clean_up_text(vacancy.css(".vacancy-serp-item__compensation::text").get()),
                "firm": clean_up_text(vacancy.css(".vacancy-serp-item__meta-info a::text").get()),
                "place": clean_up_place(vacancy.css("span.vacancy-serp-item__meta-info::text").get()),
                "url": clean_up_text(vacancy.css(".resume-search-item__name a::attr(href)").get()),
            }

        for a in response.css("a.bloko-button.HH-Pager-Controls-Next.HH-Pager-Control"):
            yield response.follow(a, callback=self.parse)
