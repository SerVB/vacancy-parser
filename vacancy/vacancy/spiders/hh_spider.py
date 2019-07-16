# -*- coding: utf-8 -*-

from typing import Optional

from scrapy import Spider
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


RUSSIA_WITH_SALARY = "https://hh.ru/search/vacancy?area=113&clusters=true&enable_snippets=true&only_with_salary=true"


class MyJsonItemExporter(JsonItemExporter):
    def __init__(self, file, **kwargs):
        super(MyJsonItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)


class HhSpider(Spider):
    MAX_PAGES = 100
    VACANCY_PER_PAGE = 20

    name = "hh"
    start_urls = [RUSSIA_WITH_SALARY]
    custom_settings = {
        # "DEPTH_LIMIT": 5,
        "FEED_EXPORTERS": {"json": "vacancy.spiders.hh_spider.MyJsonItemExporter"},
        # "ITEM_PIPELINES": {'vacancy.pipelines.DbPipeline': 300},
    }

    scraped_pages = set()

    @staticmethod
    def vacancy_count(response: Response) -> int:
        vacancy_count_str = response.css(".header.HH-SearchVacancyDropClusters-Header::text").get()
        return int("".join(filter(lambda c: c.isdigit(), vacancy_count_str)))

    def split_clusters_group(self, response: Response, group_name: str, callback) -> None:
        count = self.vacancy_count(response)
        if count <= 0.95 * self.MAX_PAGES * self.VACANCY_PER_PAGE:
            # Если уже малое число вакансий, не стоит делить дальше,
            # т.к. вакансии не всегда отнесены к категории, и при разделении могут теряться
            yield from self.parse_vacancy_page(response)
            return

        cluster_groups = response.css(".clusters-group")
        for cluster_group in cluster_groups:
            if clean_up_text(cluster_group.css(".clusters-group-title::text").get()) == group_name:
                for item in cluster_group.css(
                        "a.clusters-value:not(.clusters-value_selected):not(.clusters-list__item_more)"):
                    yield response.follow(item, callback=callback)
                return

        yield from callback(response)
        # raise Exception("Не найден кластер '%s' на %s" % (group_name, response.url))

    def split_to_regions(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "Регион", self.split_to_field)

    parse = split_to_regions

    def split_to_field(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "Профобласть", self.split_to_specialty)

    def split_to_specialty(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "Специализация", self.split_to_experience)

    def split_to_experience(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "Опыт работы", self.split_to_industry)

    def split_to_industry(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "Отрасль компании", self.split_to_schedule)

    def split_to_schedule(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "График работы", self.split_to_sub_industry)

    def split_to_sub_industry(self, response: Response) -> None:
        yield from self.split_clusters_group(response, "Сфера компании", self.parse_vacancy_page)

    def parse_vacancy_page(self, response: Response) -> None:
        count = self.vacancy_count(response)
        if count > self.MAX_PAGES * self.VACANCY_PER_PAGE:
            raise Exception("Не могу спарсить большое число вакансий (%s) на %s" % (count, response.url))

        for vacancy in response.css(".vacancy-serp-item"):
            for url in vacancy.css(".resume-search-item__name a"):
                follow = response.follow(url, callback=self.parse_vacancy)
                follow.meta["place"] = clean_up_place(vacancy.css("span.vacancy-serp-item__meta-info::text").get())
                yield follow

        for a in response.css("a.bloko-button.HH-Pager-Controls-Next.HH-Pager-Control"):
            yield response.follow(a, callback=self.parse_vacancy_page)

    def parse_vacancy(self, response: Response) -> None:
        description = clean_up_text("\n".join(response.css(".vacancy-section")[0]
                                              .css("*:not(style):not(script)::text").extract()))

        yield {
            "title": clean_up_text(response.css("h1.header::text").get()),
            "salary": clean_up_text(response.css("p.vacancy-salary::text").get()),
            "firm": clean_up_text(response.css(".vacancy-company-name span::text").get()),
            "place": response.meta["place"],
            "url": response.url,
            "description": description
        }
