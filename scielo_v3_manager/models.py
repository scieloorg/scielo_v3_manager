from datetime import datetime

from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
)
from mongoengine import (
    Document,
    StringField,
    DictField,
    ListField,
    DateTimeField,
    connect,
)


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def db_connect_by_uri(uri, **extra_dejson):
    """
    mongodb://{login}:{password}@{host}:{port}/{database}
    """
    connect(host=uri, **extra_dejson)


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def db_connect(host, port, schema, login, password, **extra_dejson):
    uri = "mongodb://{creds}{host}{port}/{database}".format(
        creds="{}:{}@".format(login, password) if login else "",
        host=host,
        port="" if port is None else ":{}".format(port),
        database=schema,
    )

    connect(host=uri, **extra_dejson)


class DocsIds(Document):
    _id = StringField(max_length=32, primary_key=True, required=True)
    v1 = StringField(max_length=23, required=False)
    v2 = StringField(max_length=23, required=True)
    v3 = StringField(max_length=23, required=False)
    aop = StringField(max_length=23, required=False)
    others = ListField()
    doi = StringField()
    filename = StringField()

    prefixes = ListField()
    fields = DictField()

    status = StringField()

    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'collection': 'documents_pids',
        'indexes': [
            'v3',
            'v1',
            'v2',
            'doi',
            'aop',
            'others',
            'fields',
            'status',
            'filename',
            'prefixes',
        ]
    }

    def __unicode__(self):
        return str({
            "_id": self._id,
            "status": self.status,
            "filename": self.filename,
            "doi": self.doi,
            "v3": self.v3,
            "v2": self.v2,
            "aop": self.aop,
            "v1": self.v1,
            "others": self.others,
            "updated": self.updated,
        })

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = datetime.utcnow().isoformat()
        if not self.updated:
            self.updated = datetime.utcnow().isoformat()

        return super(DocsIds, self).save(*args, **kwargs)
