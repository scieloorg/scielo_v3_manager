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
def db_connect_by_uri(uri):
    """
    mongodb://{login}:{password}@{host}:{port}/{database}
    """
    conn = connect(host=uri)
    print("%s connected" % uri)
    return conn


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def db_connect(host, port, schema, login, password, **extra_dejson):
    uri = "mongodb://{creds}{host}{port}/{database}".format(
        creds="{}:{}@".format(login, password) if login else "",
        host=host,
        port="" if port is None else ":{}".format(port),
        database=schema,
    )

    return connect(host=uri, **extra_dejson)


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
            self.created = datetime.utcnow().isoformat().replace("T", " ")
        self.updated = datetime.utcnow().isoformat().replace("T", " ")

        return super(DocsIds, self).save(*args, **kwargs)


def create_obj(_id, doi, filename, v2, aop, v3, status, v1, others, fields):
    if not v3 or not v2:
        raise ValueError(
            "PID manager missing v2 or v3: %s" % str((v2, v3)))
    obj = DocsIds()
    obj._id = _id
    obj.doi = doi or ""
    obj.filename = filename or ""
    obj.v1 = v1 or ""
    obj.v2 = v2
    obj.v3 = v3
    obj.aop = aop or ""
    obj.others = others or []
    obj.fields = fields or {}
    obj.status = status or "active"
    obj.prefixes = [v[:-5] for v in (v2, aop) if v]
    return obj


def complete_data(
        obj, _id, doi, filename, v2, aop, v3, status, v1, others, fields):

    obj.doi = doi or obj.doi or ""
    obj.filename = filename or obj.filename or ""
    obj.v1 = v1 or obj.v1 or ""
    obj.v2 = v2 or obj.v2 or ""
    obj.v3 = v3 or obj.v3 or ""
    obj.aop = aop or obj.aop or ""

    obj.others = others or obj.others or []
    obj.fields = fields or obj.fields or {}
    obj.status = status or obj.status or "active"
    obj.prefixes = [v[:-5] for v in (obj.v2, obj.aop) if v]
    return obj
