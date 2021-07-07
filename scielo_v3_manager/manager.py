from .models import DocsIds, db_connect_by_uri, complete_data
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


def register(doi, filename, v2, aop, v3, status, v1, others, fields):
    try:
        # obtém o registro na base de dados
        obj = get(doi, filename, v2, aop, v3)

        if not obj:
            # cria um registro novo
            obj = DocsIds()
            v3 = v3 or generates()

        # atualiza os dados
        complete_data(
            obj, doi, filename, v2, aop, v3, status, v1, others, fields)

        # salva o registro
        if not obj.v3 or not obj.v2:
            raise ValueError(
                "PID manager missing v2 or v3: %s" % str((obj.v2, obj.v3)))

        obj.save()
        return obj
    except Exception as e:
        print(e)
