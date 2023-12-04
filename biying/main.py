import hashlib
import re
import time
from datetime import datetime
from typing import Any, List, Dict
from urllib.parse import urljoin

import pymongo
import requests
from loguru import logger
from scrapy import Selector

client = pymongo.MongoClient(f'mongodb://{""}:{""}@{"127.0.0.1"}:{27017}/?authSource={"admin"}')
coll = client['mul_admin']['today_bing']

base_url = "https://www.todaybing.com/"
list_api_url = "https://www.todaybing.com/web/api"

headers = {
    "referer": "https://www.todaybing.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
}

form_headers = dict(headers, **{
    "x-requested-with": "XMLHttpRequest",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8"
})

STATE = True  # 表示可以继续向下采集


def md5_(s: str):
    """
    数据 md5
    :return:
    """
    return hashlib.md5(s.encode(encoding='UTF-8')).hexdigest()


def date_formatter(s: str):
    """
        格式化字符串日期
    :param s:
    :return:
    """
    if s:
        t = time.strptime(s, "%Y年%m月%d日")
        date1 = datetime(t[0], t[1], t[2])
        return date1.strftime('%Y-%m-%d')
    return ""


def clean_html(html: str):
    """
        处理html
    :param html:
    :return:
    """
    html = html.strip('"')
    html = html.replace('\\r', '').replace('\\n', '').replace('\\t', '')
    html = html.replace('\\"', '"').replace('\\/', '/')
    return html


def get_style_image(image: str):
    """
        获取 形如：background-image:url('https://tvax4.sinaimg.cn/wap800/bfe05ea9ly1fxgv91gzhnj21hc0u0thv') 的图片信息
    :return:
    """
    if image:
        return re.search(r'url\((.*?)\)', image).group(1).replace("'", '')


def get_pre_and_next_image(selector: Selector, xpath_arg: str, text: str):
    """
        获取上，下 篇图片
    :param xpath_arg: xpath的参数
    :param text: 详情页html
    :param selector:
    :return:
    """
    content_type = 'pre'
    if xpath_arg == "下一篇":
        content_type = 'next'
    next_pre_selector = selector.xpath(f'//div[contains(text(), "{xpath_arg}")]/../..')
    if xpath_arg in text:
        image = next_pre_selector.xpath('.//div[@class="media-content"]/@style').get('')  # 上一篇背景图片
        image = get_style_image(image)
        url = next_pre_selector.xpath('./a/@href').get('')  # 上一篇详情url
        url = urljoin(base_url, url)
        title = next_pre_selector.xpath('./a/@title').get('').strip()  # 标题
        return {
            "image": image,
            "url": url,
            "title": title,
            "content_type": content_type
        }


def parse_api(paged: int):
    """
        解析api 接口，获取列表数据
    :return:
    """
    list_api_data = {
        "append": "list-home",
        "paged": paged,  # 最多只能翻到第10页
        "action": "ajax_load_posts",
        "tabcid": 1,
    }
    logger.info(f"请求第{paged}页")
    res = requests.post(list_api_url, headers=form_headers, data=list_api_data)
    res.encoding = 'utf-8'
    if res.status_code == 200:
        json_data = res.json()
        html = json_data.get("data")
        html = clean_html(html)
        selector: Selector = Selector(text=html)
        lists: List[Selector] = selector.xpath('//div[contains(@class, "list-item")]')
        logger.info(f"获取到{len(lists)}条数据")
        parse_list_item(lists)
    else:
        logger.error(f"状态码异常, Status_Code: {res.status_code}, Res: {res.text}")


def parse_list_item(lists: List[Selector]):
    """
        解析列表数据
    :return:
    """
    global STATE
    num = 0
    for i in lists:
        url = urljoin(base_url, i.xpath('.//a[@class="media-content"]/@href').get(''))  # 详情页url
        title = i.xpath('.//a[@class="media-content"]/@title').get('').strip()  # 标题
        image = i.xpath('.//a[@class="media-content"]/@style').get('')  # 背景图像
        image = re.search(r'url\((.*?)\)', image).group(1)
        url_md5 = md5_(url)
        if coll.find_one({"hash": url_md5}):
            logger.warning(f'数据存在跳过, url={url}')
            num += 1
            continue
        dic = {
            "url": url,
            "title": title,
            "image": image,
        }
        parse_item_detail(url, dic)
    logger.info(f"页面数据{len(lists)}条，跳过: {num}条")
    if num != 0 and num <= len(lists):
        STATE = False


def get_recommend(selector: Selector):
    """
        获取推荐图片
    :param selector:
    :return:
    """
    # 推荐图片
    rec_selector = selector.xpath('//div[contains(@class,"list-grouped")]//div[contains(@class, "col-6")]')
    recommend = []
    for r in rec_selector:
        url = urljoin(base_url, r.xpath('.//a[@class="media-content"]/@href').get(''))
        image = r.xpath('.//a[@class="media-content"]/@style').get('')  # 背景图像
        image = get_style_image(image)
        title = r.xpath('.//a/@title').get('').strip()  # 标题
        recommend.append({
            "url": url,
            "image": image,
            "title": title
        })
    return recommend


def get_download_info(url: str):
    """
        获取下载地址信息
    :param url: 详情页地址
    :return:
    """
    aid = re.search(r'.*/(.*?)\.html', url).group(1).strip()
    data = {
        "aid": aid,
        "post_type": 1,
        "area": "cn",
        "action": "ajax_get_durls"
    }
    res = requests.post(list_api_url, headers=form_headers, data=data)
    if res.status_code == 200:
        logger.info(f"获取{url}页面下载信息成功")
        return res.json()
    else:
        logger.error(f"获取{url}页面下载信息失败")
        return {}


def parse_item_detail(url, data: Dict[str, Any]):
    """
        请求并解析详情页
    :param url:
    :param data:
    :return:
    """
    logger.info(f"正在请求详情页: url={url}")
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    if res.status_code == 200:
        text = res.text
        selector = Selector(text=text)
        author = selector.xpath("//a[@class='author-popup']/text()").get('').strip()  # 作者
        site = selector.xpath("//i[contains(@class, 'icon-map')]/../text()").get('').strip()  # 地点
        introduce: List[str] = selector.xpath("//div[contains(@class, 'post-content')]//text()").getall()  # 介绍
        pub_date = date_formatter(selector.xpath("//meta[@itemprop='dateUpdate']/@content").get(''))  # 发布日期
        if introduce:
            introduce = [i for i in introduce if '查看译文' not in i and '现在登录' not in i]
        hd_image: str = selector.xpath("//img[@id='mbg']/@src").get('')  # 高清图
        pre_data = get_pre_and_next_image(selector, "上一篇", text)
        next_data = get_pre_and_next_image(selector, "下一篇", text)
        next_prev_data = []
        if next_data:
            next_prev_data.append(next_data)
        if pre_data:
            next_prev_data.append(pre_data)
        recommend = get_recommend(selector)  # 推荐图片
        download_info = get_download_info(url)  # 获取下载4k，VIP图片信息
        result = dict({
            "hash": md5_(url),
            "author": author,
            "site": site,
            "pub_date": pub_date,
            "introduce": introduce,
            "hd_image": hd_image,
            "next_prev_data": next_prev_data,
            "recommend": recommend,
            "download_info": download_info,
            "download_time": int(round(time.time() * 1000))
        }, **data)
        coll.insert_one(result)
        logger.info(f"数据插入成功, detail_url={url}")
    else:
        logger.error(f"详情页请求失败, detail_url={url}")


if __name__ == '__main__':
    for i in range(1):
        if STATE is False:
            logger.warning(f"程序运行结束")
            break
        parse_api(i + 1)
    logger.warning(f"程序运行结束")
