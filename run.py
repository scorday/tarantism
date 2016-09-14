import hashlib
import logging
from random import choice, sample
from threading import Thread
from time import time, sleep
from uuid import uuid4

import ujson

from datetime import datetime, timedelta

from tarantool import DatabaseError
from tarantool.const import *
from tqdm import tqdm

from tarantism import models, connect, fields, disconnect
from tarantism.exceptions import IndexExists

names = '''
Lorem ipsum dolor sit amet, consectetur adipiscing elit,
sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
Ut enim ad minim veniam, quis nostrud exercitation ullamco
laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit
esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat
non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
'''.replace('\n', '').split(' ')

names = [i for i in names if i]


def smart_str(s, encoding='utf-8', errors='strict'):
    if isinstance(s, unicode):
        return s.encode(encoding, errors)
    elif s and encoding != 'utf-8':
        return s.decode('utf-8', errors).encode(encoding, errors)
    else:
        return s


def get_hexdigest(s, salt='', algorithm='md5'):
    return hashlib.new(algorithm, smart_str(salt + s)).hexdigest()


class Card(models.Model):
    meta = {
        'space': 'card'
    }

    id = fields.UUIDField(primary_key=True)

    url = fields.StringField(required=True, max_length=2048)
    url_hash = fields.StringField(required=True, max_length=32)

    project_id = fields.IntField()
    request_id = fields.StringField()
    source_id = fields.IntField()

    block_ids = fields.ListAsDictField(fields.IntField())

    is_rubricated = fields.BooleanField(default=False)
    rubric_ids = fields.ListAsDictField(fields.IntField())

    is_junked = fields.BooleanField(default=False)

    importance = fields.IntField()
    created_at = fields.DateTimeField(default=datetime.utcnow, required=True)
    updated_at = fields.DateTimeField(default=datetime.utcnow, required=True)

    data_hash = fields.StringField(max_length=32)

    sentiment = fields.StringField(max_length=32, default='undefined')
    sentiment_details = fields.DictField()

    objectivity = fields.BooleanField()
    objectivity_details = fields.DictField()

    highlights = fields.StringField()
    document_url = fields.StringField()

    is_published = fields.BooleanField(default=False)
    published_at = fields.DateTimeField()
    published_by = fields.IntField()

    is_duplicate = fields.BooleanField()
    original_id = fields.StringField()
    dupl_count = fields.IntField(default=0)
    shingles = fields.ListField(fields.IntField())
    percentage = fields.IntField()

    # coords = GeoField()
    language = fields.StringField(max_length=5)

    external_id = fields.StringField()
    spider_host = fields.StringField()
    spider_version = fields.StringField()


class CardData(models.Model):
    meta = {
        'space': 'card_data',
        'space_args': (dict(engine='vinyl'),)
    }

    id = fields.UUIDField(primary_key=True)
    data = fields.DictField()


conn = connect(
    host='localhost',
    port=3301,
    user='avl',
    password='avl',
)

Card.create_space()
CardData.create_space()

try:
    indexes = [
        ['project_id', 'is_junked', 'created_at'],
        ['project_id', 'is_junked', 'is_rubricated', 'created_at'],
        ['project_id', 'is_junked', 'is_published', 'published_at'],
    ]

    CardData.create_index(
        index_name='id',
        index_type='tree',
        fields=['id'],
        unique=True
    )

    Card.create_index(
        index_name='id',
        index_type='hash',
        fields=['id'],
        unique=True
    )

    Card.create_index(
        index_type='tree',
        fields=['project_id', 'source_id', 'url_hash'],
        unique=False
    )

    Card.create_index(
        index_type='tree',
        fields=['project_id', 'source_id', 'data_hash'],
        unique=False
    )

    for i, fields in enumerate(indexes):
        Card.create_index(
            index_name='idx_' + str(i),
            index_type='tree',
            fields=fields,
            unique=False
        )
except IndexExists:
    pass


disconnect()


stats = {}


def runner(id_thread):
    logging.info('Thread %s: start.' % id_thread)

    global stats

    t0 = time()
    total = 20 * 10 ** 6
    startdt = datetime.utcnow() - timedelta(seconds=total)

    sleep_done = False

    connect(
        host='localhost',
        port=3301,
        user='avl',
        password='avl',
    )

    try:
        for i in xrange(1, total):
            url = '/'.join(sample(names, 3))
            data = dict(
                title=' '.join(sample(names, 5)),
                description=' '.join(sample(names, 10)),
                text=' '.join(sample(names, 30))
            )

            card = Card(
                url=url,
                url_hash=get_hexdigest(url),
                data_hash=get_hexdigest(ujson.dumps(sorted(data.items()))),
                project_id=choice(xrange(0, 10)),
                source_id=choice(xrange(0, 1000)),
                block_ids=sample(xrange(0, 100), choice(xrange(0, 5))),
                rubric_ids=sample(xrange(0, 100), choice(xrange(0, 5))),
                is_rubricated=choice([True, False]),
                is_junked=choice([True, False]),
                is_published=choice([True, False]),
                is_duplicate=choice([True, False]),
                created_at=startdt + timedelta(seconds=i),
                published_at=startdt + timedelta(seconds=i)
            )

            card_data = CardData(
                id=card.id,
                data=data
            )

            card.save()
            card_data.save()

            if i % 1000 == 0:
                logging.info('Thread %s: speed: %s' % (id_thread, i / float(time()-t0)))
                stats[id_thread] = (i, time()-t0)
                if i / float(time()-t0) < 100:
                    if not sleep_done:
                        logging.info('Thread %s: sleep.' % id_thread)
                        sleep(30)
                        sleep_done = True
                    else:
                        sleep_done = False
    except Exception as e:
        print e

    logging.info('Thread %s: stop.' % id_thread)


t0 = time()
threads = []
for i in range(5):
    th = Thread(target=runner, args=(i,))
    th.start()
    threads.append(th)

for th in threads:
    th.join()

    print 'total:', Card.get_space().count()
    print 'total time:', time() - t0

# t0 = time()
# total = 20*10**6
# startdt = datetime.utcnow() - timedelta(seconds=total)
#
# try:
#     for i in tqdm(xrange(0, total), total=total):
#         url = '/'.join(sample(names, 3))
#         data = dict(
#             title=' '.join(sample(names, 5)),
#             description=' '.join(sample(names, 10)),
#             text=' '.join(sample(names, 30))
#         )
#
#         card = Card(
#             url=url,
#             url_hash=get_hexdigest(url),
#             data_hash=get_hexdigest(ujson.dumps(sorted(data.items()))),
#             project_id=choice(xrange(0, 10)),
#             source_id=choice(xrange(0, 1000)),
#             block_ids=sample(xrange(0, 100), choice(xrange(0, 5))),
#             rubric_ids=sample(xrange(0, 100), choice(xrange(0, 5))),
#             is_rubricated=choice([True, False]),
#             is_junked=choice([True, False]),
#             is_published=choice([True, False]),
#             is_duplicate=choice([True, False]),
#             created_at=startdt + timedelta(seconds=i),
#             published_at=startdt + timedelta(seconds=i)
#         )
#
#         card_data = CardData(
#             id=card.id,
#             data=data
#         )
#
#         card.save()
#         card_data.save()
# except Exception as e:
#     print e
#
#
# print 'total:', Card.get_space().count()
# print 'total time:', time() - t0


    #
    #
    #
    #
    # t0 = time()
    # total = 10**6
    # for i in tqdm(xrange(0, total), total=total):
    #     Card.objects(id=choice(ids))
    # print 'total time:', time()-t0
    #
    #
    # t0 = time()
    # total = 10**3
    # for i in tqdm(xrange(0, total), total=total):
    #     name_index.count(choice(names))
    # print 'total time:', time()-t0


    # total: - [4617254]
    # total time: 1939.91535497

    # total: - [2890165]
    # total time: 1295.43082905


# total: - [1347265]
# total time: 788.11080718


# total: - [530880]
# total time: 562.815409899