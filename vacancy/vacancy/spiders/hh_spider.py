# -*- coding: utf-8 -*-

import logging
from typing import Optional, List

from scrapy import Spider, Request
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
MOSCOW_WITH_SALARY = "https://hh.ru/search/vacancy?clusters=true&enable_snippets=true&only_with_salary=true&area=1&" \
                     "from=cluster_area&showClusters=true"


class MyJsonItemExporter(JsonItemExporter):
    def __init__(self, file, **kwargs):
        super(MyJsonItemExporter, self).__init__(file, ensure_ascii=False, **kwargs)


class HhSpider(Spider):
    MAX_PAGES = 100
    VACANCY_PER_PAGE = 20

    name = "hh"
    custom_settings = {
        # "DEPTH_LIMIT": 5,
        "FEED_EXPORTERS": {"json": "vacancy.spiders.hh_spider.MyJsonItemExporter"},
        # "ITEM_PIPELINES": {'vacancy.pipelines.DbPipeline': 300},
    }

    max_count = 0
    scrapped = 0

    def closed(self, reason):
        logging.warning("Примерно потеряно вакансий: %s" % (self.max_count - self.scrapped))

    def start_requests(self) -> List[Request]:
        return [
            Request(
                url=RUSSIA_WITH_SALARY,
                callback=self.split_clusters_group,
                meta={"split_by": frozenset((
                    "Регион",
                    "Профобласть",
                    "Специализация",
                    "Опыт работы",
                    "Отрасль компании",
                    "График работы",
                    "Сфера компании",
                    "Тип занятости",
                ))}
            ),
            # Request(
            #     url=MOSCOW_WITH_SALARY,
            #     callback=self.split_clusters_group,
            #     meta={"split_by": frozenset((
            #         "Профобласть",
            #         "Специализация",
            #         "Опыт работы",
            #         "Отрасль компании",
            #         "График работы",
            #         "Сфера компании",
            #         "Тип занятости",
            #     ))}
            # ),
        ]

    def vacancy_count(self, response: Response) -> int:
        vacancy_count_str = response.css(".header.HH-SearchVacancyDropClusters-Header::text").get()
        vacancy_count_int = int("".join(filter(lambda c: c.isdigit(), vacancy_count_str)))
        self.max_count = max(self.max_count, vacancy_count_int)
        return vacancy_count_int

    def split_clusters_group(self, response: Response) -> None:
        count = self.vacancy_count(response)  # количество вакансий вверху страницы
        if count <= 0.95 * self.MAX_PAGES * self.VACANCY_PER_PAGE:
            # Если уже малое число вакансий, не стоит делить дальше,
            # т.к. вакансии не всегда отнесены к категории, и при разделении могут теряться
            yield from self.parse_vacancy_page(response)
            return

        # сначала пробуем разбивать точно:
        cluster_groups = response.css(".clusters-group")
        for cluster_group in cluster_groups:
            split = clean_up_text(cluster_group.css(".clusters-group-title::text").get())
            if split in response.meta["split_by"]:
                sum_count = 0  # сумма вакансий внутри списка
                for item in cluster_group.css("a.clusters-value .clusters-value__count::text"):
                    sum_count += int("".join(filter(lambda c: c.isdigit(), item.get())))

                if sum_count == count:  # можно разбить точно?
                    for item in cluster_group.css(
                            "a.clusters-value:not(.clusters-value_selected):not(.clusters-list__item_more)"):
                        follow = response.follow(item, callback=self.split_clusters_group)
                        follow.meta["split_by"] = frozenset(response.meta["split_by"] - frozenset({split}))
                        yield follow
                    return

        # если не получилось, пробуем разбить хоть как-то:
        for cluster_group in cluster_groups:
            split = clean_up_text(cluster_group.css(".clusters-group-title::text").get())
            if split in response.meta["split_by"]:
                for item in cluster_group.css(
                        "a.clusters-value:not(.clusters-value_selected):not(.clusters-list__item_more)"):
                    follow = response.follow(item, callback=self.split_clusters_group)
                    follow.meta["split_by"] = frozenset(response.meta["split_by"] - frozenset({split}))
                    yield follow

        yield from self.parse_vacancy_page(response)

    def parse_vacancy_page(self, response: Response) -> None:
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

        self.scrapped += 1

        yield {
            "title": clean_up_text(response.css(".vacancy-title h1.header::text").get()),
            "salary": clean_up_text(response.css("p.vacancy-salary::text").get()),
            "firm": clean_up_text(response.css(".vacancy-company-name span::text").get()),
            "place": response.meta["place"],
            "url": response.url,
            "description": description
        }
