from .models import DocsIds, db_connect_by_uri
from .v3_gen import generates


def get_by_doi(doi, filename):
    if doi:
        return (
            DocsIds.objects(doi=doi, filename=filename) or
            DocsIds.objects(doi=doi)
        )


def get_by_v3(v3):
    if v3:
        return (
            DocsIds.objects(pk=v3) or
            DocsIds.objects(v3=v3) or
            DocsIds.objects(others=v3)
        )


def get_by_v2(v2, filename):
    if v2:
        return (
            DocsIds.objects(prefixes=v2[:-5], filename=filename) or
            DocsIds.objects(v2=v2, filename=filename) or
            DocsIds.objects(aop=v2, filename=filename) or
            DocsIds.objects(others=v2, filename=filename)
        )


def get(doi, filename, v2, aop=None, v3=None):
    records = (
        get_by_doi(doi, filename) or
        get_by_v2(aop, filename) or
        get_by_v2(v2, filename) or
        get_by_v3(v3)
    )
    v2_items = [i for i in (v2, aop) if i]
    for rec in records or []:
        if (rec.v2 in v2_items or rec.aop in v2_items or rec._id == v3):
            return rec


def _update(record, doi, filename, v2, aop, v3, status, v1, others, fields):
    if not v3 or not v2:
        raise ValueError("PID manager missing v2 or v3: %s" % str((v2, v3)))
    record._id = v3
    record.doi = doi
    record.filename = filename
    record.v1 = v1 or ""
    record.v2 = v2
    record.v3 = v3
    record.aop = aop or ""
    record.others = others or []
    record.fields = fields or {}
    record.status = status or "active"
    record.prefixes = [v[:-5] for v in (v2, aop) if v]


def register(doi, filename, v2, aop, v3, status, v1, others, fields,
             generate_v3=generates):
    obj = get(doi, filename, v2, aop, v3)
    if not obj:
        obj = DocsIds()

    v3 = v3 or generate_v3 and generate_v3()
    _update(obj, doi, filename, v2, aop, v3, status, v1, others, fields)

    obj.save()
    return obj


def connect(uri_and_db, **extra_dejson):
    db_connect_by_uri(uri_and_db, **extra_dejson)

