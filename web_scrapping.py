import sys
from bs4 import BeautifulSoup as bSoup
import requests
import traceback
from requests import HTTPError, ConnectTimeout, TooManyRedirects, RequestException
from dateutil.parser import parse

AMAZON = "https://www.amazon.co.uk"
# URL_PRODUCT = "https://www.amazon.co.uk/Apple-iPhone-14-Plus-128/dp/B0BDJY2DFH/ref=sr_1_2_sspa?" \
#               "keywords=iphone+14+pro+max&qid=1670164444&" \
#               "sprefix=iphone+%2Caps%2C83&sr=8-2-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&psc=1"

"""
    HTTP request headers, to simulate a browser request, otherwise we won't the entire website content
    because websites will thinkg the request is being sent by a robot.
"""

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
}


def get_the_soup(url):
    """
          Function that gets the DOM of the website associated to the url and wrap it into a BS object which is return
          to the caller.
          :param url: Amazon's Website product URL
          :return: BeautifulSoup object with the DOM of the website loaded in it.
    """
    try:
        dom = requests.get(url, headers=HEADERS)
        product_soup = bSoup(dom.content, "html.parser", from_encoding="UTF-8")
        return product_soup
    except ConnectionError:
        print("I'm sorry but there was a connection problem, please check your connection... :(")
        traceback.print_exc()
        sys.exit(-1)
    except HTTPError:
        print("I'm sorry but we couln't connect with the give URL, please check it and try again")
        traceback.print_exc()
        sys.exit(-1)
    except ConnectTimeout:
        print("Connection Timeout... :(")
        traceback.print_exc()
        sys.exit(-1)
    except TooManyRedirects:
        print("We've exceed the numer of redirections allowed... :(")
        traceback.print_exc()
        sys.exit(-1)
    except RequestException:
        print("Something went wrong with the request... :(")
        traceback.print_exc()
        sys.exit(-1)
    except Exception:
        print("Something went wrong, please check the traceback to get more details about it... :(")
        traceback.print_exc()
        sys.exit(-1)


def get_product_title(product_soup):
    """
        :param product_soup: BeautifulSoup object with the DOM of the website loaded in it.
        :return: title of the Amazon product
    """
    title = product_soup.find("span", {"id": "productTitle"})
    return title.text.strip()


def get_product_price(product_soup):
    """
        :param product_soup: BeautifulSoup object with the DOM of the website loaded in it.
        :return: price of the Amazon product
    """
    price = product_soup.find("span", {"class": "a-offscreen"})
    return price.text.strip()


def get_product_reviews_text(reviews_soup):
    """
    :param reviews_soup:Soup object with a list of the reviews
    :return: List with the review text given to every review
    """
    reviews_body = []
    for review in reviews_soup:
        body = review.find("span", {"data-hook": "review-body"})
        reviews_body.append(body.span.text)
    return reviews_body


def get_product_reviews_stars(reviews_soup):
    """
    :param reviews_soup: Soup object with a list of the reviews
    :return: List with the number of start given to every review
    """
    reviews_stars = []
    for review in reviews_soup:
        stars = review.find("span", {"class": "a-icon-alt"})
        reviews_stars.append(stars.text.split(" ")[0])
    return reviews_stars


def get_product_reviews_country(reviews_soup):
    """
    :param reviews_soup: Soup object with a list of the reviews
    :return: List with the countries where the reviews were given from
    """
    reviews_country = []
    for review in reviews_soup:
        country = review.find("span", {"data-hook": "review-date"})
        countries_tmp = country.text.split(" ")[2:-4]
        reviews_country.append(" ".join(countries_tmp))
    return reviews_country


def parse_date(date):
    """

    :param date:in amazon's website reviews format
    :return: same date in format dd/mm/Year
    """
    dt = parse(date)
    return dt.strftime("%d/%m/%Y")


def get_product_reviews_date(reviews_soup):
    """
    :param reviews_soup: Soup object with a list of the reviews
    :return: List with the date when the reviews were given
    """
    reviews_date = []
    for review in reviews_soup:
        date = review.find("span", {"data-hook": "review-date"})
        reviews_date.append(parse_date((" ".join(date.text.split(" ")[-3:]))))
    return reviews_date


def get_product_reviews_size(reviews_soup):
    """
    :param reviews_soup: Soup object with a list of the reviews
    :return: List with the size of the product associated to the review
    """
    reviews_size = []
    for review in reviews_soup:
        size = review.find("a", {"data-hook": "format-strip"})
        if size is not None:
            reviews_size.append(size.contents[0].split(" ")[2])
        else:
            reviews_size.append("")
    return reviews_size


def get_product_reviews_colour(reviews_soup):
    """
    :param reviews_soup: Soup object with a list of the reviews
    :return: List with the colour of the product associated to the review
    """
    reviews_colour = []
    for review in reviews_soup:
        colour = review.find("a", {"data-hook": "format-strip"})
        if colour is not None:
            reviews_colour.append(colour.contents[-1].split(" ")[-1])
        else:
            reviews_colour.append("")
    return reviews_colour


def get_product_reviews_num_pages(reviews_soup):
    """
    :param reviews_soup: soup object that contains the number of ratings with reviews
    :return: num of pages with reviews
    """
    num_reviews = int(reviews_soup.text.strip().split(" ")[3])
    if num_reviews % 10 == 0:
        return num_reviews / 10
    else:
        return num_reviews // 10 + 1


def get_product_reviews(product_soup):
    """
    :param product_soup:  BeautifulSoup object with the DOM of the website loaded in it.
    :return: List of all the reviews given to the product
    """
    try:
        product_title = get_product_title(product_soup)
        product_price = get_product_price(product_soup)
        list_reviews = []
        link_all_reviews = product_soup.find("a", {"data-hook": "see-all-reviews-link-foot"})
        reviews_soup = get_the_soup(AMAZON+link_all_reviews["href"])
        num_pages = get_product_reviews_num_pages(
            reviews_soup.find("div", {"data-hook": "cr-filter-info-review-rating-count"}))
        for i in range(1, int(num_pages)):
            reviews_text = get_product_reviews_text(reviews_soup.find_all("div", {"data-hook": "review"}))
            reviews_stars = get_product_reviews_stars(reviews_soup.find_all("div", {"data-hook": "review"}))
            reviews_countries = get_product_reviews_country(reviews_soup.find_all("div", {"data-hook": "review"}))
            reviews_date = get_product_reviews_date(reviews_soup.find_all("div", {"data-hook": "review"}))
            reviews_size = get_product_reviews_size(reviews_soup.find_all("div", {"data-hook": "review"}))
            reviews_colour = get_product_reviews_colour(reviews_soup.find_all("div", {"data-hook": "review"}))
            for j in range(len(reviews_text)):
                reviews_data = dict()
                reviews_data["title"] = product_title[0:-19]
                reviews_data["price"] = product_price[1:]
                reviews_data["text"] = reviews_text[j]
                reviews_data["stars"] = reviews_stars[j]
                reviews_data["country"] = reviews_countries[j]
                reviews_data["date"] = reviews_date[j]
                reviews_data["size"] = reviews_size[j]
                reviews_data["colour"] = reviews_colour[j]
                list_reviews.append(reviews_data)
            link_next_page = (AMAZON + reviews_soup.find("li", {"class": "a-last"}).find("a")["href"])
            reviews_soup = get_the_soup(link_next_page)
        return list_reviews
    except AttributeError:
        print("An attribute wasn't found at some point of the information collection process, "
              "to get more information about it, please check on the stacktrace")
        traceback.print_exc()
        sys.exit(-1)


# soup = get_the_soup(URL_PRODUCT)
# reviews = get_product_reviews(soup)
