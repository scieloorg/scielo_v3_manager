# coding: utf-8

import logging

from sqlalchemy import create_engine
from opac_schema.v1.models import Article

from .models import db_connect_by_uri
from .manage import register


mongo_options = {
    'maxIdleTimeMS': 20000,
    'socketTimeoutMS': 20000,
    'connectTimeoutMS': 20000,
}


class Migration:

    def __init__(self, sql_db, mongo_db,
                 sql_options={"connect_timeout": 20000},
                 mongo_options=mongo_options):
        self.sql_options = sql_options
        self.sql_engine = create_engine(
            sql_db,
            connect_args=sql_options)
        db_connect_by_uri(mongo_db, **mongo_options)

    def _get_pid_manager(self, v2, aop):
        logging.info("Consulta pid_versions para %s" % str((v2, aop)))
        conn = self.sql_engine.connect(close_with_result=True)
        if v2 and aop:
            sqrs = conn.execute(
                "SELECT * FROM pid_versions WHERE v2='%s' OR v2='%s'" %
                (v2, aop)
            )
        elif v2 or aop:
            sqrs = conn.execute(
                "SELECT * FROM pid_versions WHERE v2='%s'" % v2 or aop
            )
        if sqrs:
            result = None
            for row in sqrs:
                logging.info(row)
                result = row
            return result

    def _get_article(self, doi, v2, v3, aop):
        logging.info(
            "Consulta base do site novo: %s" % str((doi, v2, v3, aop)))
        article = None

        if not article and v3:
            article = (
                Article.objects(pk=v3, is_public=True) or
                Article.objects(scielo_pids__other=v3, is_public=True)
            )

        if doi:
            article = (
                Article.objects(doi=doi, is_public=True)
            )

        if not article and aop:
            article = (
                Article.objects(pid=aop, is_public=True) or
                Article.objects(aop_pid=aop, is_public=True) or
                Article.objects(scielo_pids__other=aop, is_public=True)
            )

        if not article and v2:
            article = (
                Article.objects(pid=v2, is_public=True) or
                Article.objects(aop_pid=v2, is_public=True) or
                Article.objects(scielo_pids__other=v2, is_public=True)
            )

        for a in article or []:
            if v2 in [a.pid, a.aop_pid] + a.scielo_pids.get("other") or []:
                return a
            if aop in [a.pid, a.aop_pid] + a.scielo_pids.get("other") or []:
                return a

    def migrate(self, filename, doi, v2, aop, v3):
        saved = None
        data = str((filename, v2, aop, v3))
        logging.info("ARTIGO: %s" % data)

        v3_origin = "new_site"
        article = self._get_article(doi, v2, v3, aop)
        if not article:
            record = self._get_pid_manager(v2, aop)
            if record:
                v3 = record[-1]
                v3_origin = "pid_versions"
                article = self._get_article(doi, v2, v3, aop)
        if article:
            logging.info(
                "Cria registro com os dados do site novo: %s" % str(article))
            saved = register(
                doi=article.doi,
                filename=filename,
                v2=article.pid,
                aop=article.aop_pid,
                v3=article._id,
                status="active",
                v1=article.scielo_pids.get("v1"),
                others=article.scielo_pids.get("other"),
                fields={'source': 'new'},
            )
        else:
            logging.info(
                "Cria registro com os dados do site clássico: %s" %
                str((filename, v2, aop, v3)))
            if not v3:
                v3_origin = "generated"
            saved = register(
                doi=doi,
                filename=filename,
                v2=v2,
                aop=aop,
                v3=v3,
                status="active",
                v1=None,
                others=None,
                fields={'source': 'old', 'v3_origin': v3_origin},
            )
        if not saved:
            logging.error("Não foi possível registrar: %s" % data)
            print("Não foi possível registrar: %s" % data)
        return saved
