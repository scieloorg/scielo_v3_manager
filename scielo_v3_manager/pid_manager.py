import logging
from datetime import datetime
from contextlib import contextmanager

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime,
    UniqueConstraint, create_engine,
)
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError


Base = declarative_base()

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)


class MoreThanOneRecordFoundError(Exception):
    ...


class AlreadyRegisteredDocumentError(Exception):
    ...


class RegistrationError(Exception):
    ...


class QueryAOPError(Exception):
    ...


class QueryDocumentInPidsTableError(Exception):
    ...


class QueryDocumentInPidVersionsTableError(Exception):
    ...


class QueryDocumentError(Exception):
    ...


class IsRegisteredV3Error(Exception):
    ...


class PidVersion(Base):
    __tablename__ = 'pid_versions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    v2 = Column(String(23))
    v3 = Column(String(255))
    __table_args__ = (
        UniqueConstraint('v2', 'v3', name='_v2_v3_uc'),
    )

    def __repr__(self):
        return '<PidVersion(v2="%s", v3="%s")>' % (self.v2, self.v3)


class NewPidVersion(Base):
    __tablename__ = 'pids'

    id = Column(Integer, primary_key=True, autoincrement=True)
    v2 = Column(String(23), index=True, unique=True)
    v3 = Column(String(23), index=True, unique=True)
    aop = Column(String(23), index=True)
    filename = Column(String(80), index=True)
    prefix_v2 = Column(String(18), index=True)
    prefix_aop = Column(String(18), index=True)
    doi = Column(String(80), index=True)
    status = Column(String(6))
    created = Column(DateTime, default=datetime.utcnow)
    updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('v3', name='_pids_'),
    )

    def __repr__(self):
        return (
            '<NewPidVersion(v2="%s", v3="%s", aop="%s", doi="%s", filename="%s")>' %
            (self.v2, self.v3, self.aop, self.doi, self.filename)
        )


class Documents(Base):
    __tablename__ = 'docs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    v2 = Column(String(23), index=True, nullable=False)
    v3 = Column(String(23), index=True, nullable=False)
    v3_origin = Column(String(23), nullable=False)
    aop = Column(String(23), index=True)

    filename = Column(String(50), index=True)
    doi = Column(String(50), index=True)
    issn = Column(String(9), index=True, nullable=False)
    pub_year = Column(String(4), index=True, nullable=False)
    issue_order = Column(String(4), index=True, nullable=False)
    volume = Column(String(10), index=True)
    number = Column(String(10), index=True)
    suppl = Column(String(10), index=True)
    elocation = Column(String(10), index=True)
    fpage = Column(String(10), index=True)
    lpage = Column(String(10), index=True)
    first_author_surname = Column(String(30), index=True)
    last_author_surname = Column(String(30), index=True)

    article_title = Column(String(50))
    other_pids = Column(String(200))
    status = Column(String(15))

    created = Column(DateTime, default=datetime.utcnow())
    updated = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.now())

    __table_args__ = (
        UniqueConstraint('v2', 'v3', name='_docs_'),
    )

    def __repr__(self):
        return (
            '<Documents(v2="%s", v3="%s", aop="%s", doi="%s", filename="%s")>' %
            (self.v2, self.v3, self.aop, self.doi, self.filename)
        )

    @property
    def as_dict(self):
        NAMES = (
            'v2', 'v3', 'v3_origin',
            'aop', 'filename', 'doi',
            'pub_year', 'issue_order',
            'elocation', 'fpage', 'lpage',
            'first_author_surname', 'last_author_surname',
            'article_title', 'other_pids',
            'issn',
            'id', 'updated', 'created',
        )
        values = (
            self.v2, self.v3, self.v3_origin,
            self.aop, self.filename, self.doi,
            self.pub_year, self.issue_order,
            self.elocation, self.fpage, self.lpage,
            self.first_author_surname, self.last_author_surname,
            self.article_title, self.other_pids,
            self.issn,
            self.id, str(self.updated), str(self.created),
        )
        return dict(
            zip(NAMES, values)
        )


class DocManager:

    def __init__(self, session, generate_v3,
                 v2, v3, aop, filename, doi,
                 status,
                 pub_year, issue_order,
                 volume, number, suppl,
                 elocation, fpage, lpage,
                 first_author_surname, last_author_surname,
                 article_title, other_pids):
        self._input_data = None
        self._session = session
        self._generate_v3 = generate_v3
        self._load_input(
            v2, v3, aop, filename, doi,
            status,
            pub_year, issue_order,
            volume, number, suppl,
            elocation, fpage, lpage,
            first_author_surname,
            last_author_surname,
            article_title, other_pids,
        )

    def _load_input(self, v2, v3, aop, filename, doi,
                    status,
                    pub_year, issue_order,
                    volume, number, suppl,
                    elocation, fpage, lpage,
                    first_author_surname, last_author_surname,
                    article_title, other_pids,
                    ):
        UPPER_CONTENTS = (
            'doi', 'elocation', 'fpage', 'lpage',
            'status',
            'first_author_surname', 'last_author_surname',
            'article_title',
            'volume', 'number', 'suppl',
        )

        v2 = (v2 or '')[:23]
        aop = (aop or '')[:23]
        filename = (filename or '')[:50]
        status = (status or '')[:15]
        first_author_surname = (first_author_surname or '')[:30]
        last_author_surname = (last_author_surname or '')[:30]
        article_title = (article_title or '')[:50]
        other_pids = (other_pids or '')[:200]
        issue_order = str(issue_order)
        issue_order = (issue_order[4:] or issue_order).zfill(4)

        self._input_data = {}
        self._input_data["v3_origin"] = ''
        self._input_data["v2"] = v2 or ''
        self._input_data["aop"] = aop or ''
        self._input_data["doi"] = doi or ''
        self._input_data["status"] = status or ''
        self._input_data["pub_year"] = pub_year or ''
        self._input_data["issue_order"] = issue_order or ''
        self._input_data["volume"] = volume or ''
        self._input_data["number"] = number or ''
        self._input_data["suppl"] = suppl or ''
        self._input_data["elocation"] = elocation or ''
        self._input_data["fpage"] = fpage or ''
        self._input_data["lpage"] = lpage or ''
        self._input_data["first_author_surname"] = first_author_surname or ''
        self._input_data["last_author_surname"] = last_author_surname or ''
        self._input_data["article_title"] = article_title or ''
        self._input_data["issn"] = v2[1:10] or ''
        self._input_data["v3"] = v3 or ''
        if v3:
            self._input_data["v3_origin"] = "provided"
        self._input_data["other_pids"] = other_pids or ''
        self._input_data["filename"] = filename or ''

        for label in UPPER_CONTENTS:
            self._input_data[label] = self._input_data[label].upper()

    @property
    def input_data(self):
        return self._input_data

    @property
    def _input_attributes(self):
        return (
            self._doc_and_issue_attributes + [
                'aop', 'v2', 'v3', 'other_pids',
                'article_title', 'filename',
                'volume', 'number', 'suppl',
                'status',
            ]
        )

    @property
    def _doc_attributes(self):
        return [
            'issn', 'pub_year',
            'doi', 'first_author_surname', 'last_author_surname'
        ]

    @property
    def _doc_and_issue_attributes(self):
        return (
            self._doc_attributes +
            ['issue_order', 'elocation', 'fpage', 'lpage']
        )

    def _db_query(self, table_class, **kwargs):
        return self._session.query(table_class).filter_by(**kwargs).all()

    def _get_document_published_in_an_issue(self, v2=None):
        """
        Busca pelos dados do artigo + issue:
            ['issn', 'pub_year',
             'doi', 'first_author_surname', 'last_author_surname'] +
            ['issue_order', 'elocation', 'fpage', 'lpage']
        """
        params = {
            k: self.input_data.get(k) or ''
            for k in self._doc_and_issue_attributes
        }
        if v2:
            params["v2"] = v2

        try:
            found = self._db_query(Documents, **params)
        except Exception as e:
            raise QueryDocumentError(
                "Error querying doc %s %s" % (params, e))
        try:
            return sorted(found, key=lambda item: item.id)[-1].as_dict
        except (IndexError, TypeError):
            return None

    def _get_document_aop_version(self):
        """
        Busca pela versão aop, ou seja, busca pelos dados do artigo:
            'pub_year',
            'doi',
            'first_author_surname', 'last_author_surname',
            'issn',
            e
            "volume": "", "number": "", "suppl": "", "fpage": "", "lpage": ""

        Caso não encontre, busca somente pelos dados de artigo, pois ele pode
            ter uma versão ahead of print e, por isso, não encontrou
            o documento com os dados do fascículo atual
        Verifica se o registro encontrado é único e tem dados de aop,
            então cria um registro com os dados do artigo no fascículo e
            adiciona o pid aop e reaproveita o v3
        Mas se mais de um registro for encontrado ou o registro não é de aop,
            então há um conflito, cria um novo registro com os dados atuais,
            gera um novo v3
        """
        params = {
            k: self.input_data.get(k) or ''
            for k in self._doc_attributes
        }
        params.update(
            {"volume": "", "number": "", "suppl": "", "fpage": "", "lpage": ""}
        )
        try:
            found = self._db_query(Documents, **params)
        except Exception as e:
            raise QueryAOPError("Error querying aop version %s %s" %
                                (params, e))

        if len(found) == 0:
            # não está registrado
            return None

        if len(found) == 1:
            doc = found[0]
            return {
                "aop": doc.v2, "v3": doc.v3, "v3_origin": f"docs_{doc.id}",
            }

        raise MoreThanOneRecordFoundError(
            "Found more than one: {}".format(
                " ".join([doc.id for doc in found])
            )
        )

    def _get_document_from_pids_table(self):
        """
        Busca registro na tabela `pids`:
            id
            v2
            v3
            aop
            filename
            prefix_v2
            prefix_aop
            doi
            status

        """
        try:
            found = self._session.query(NewPidVersion).filter(
                NewPidVersion.filename == (self.input_data.get("filename") or ''),
                func.upper(NewPidVersion.doi) == (self.input_data.get("doi") or ''),
            ).all()
        except Exception as e:
            raise QueryDocumentInPidsTableError(
                "Error querying doc in `pids`: %s %s %s" %
                (
                    self.input_data.get("filename"),
                    self.input_data.get("doi"),
                    e,
                )
            )
        if len(found) == 0:
            # não está registrado
            return None

        if len(found) == 1:
            d = {
                "v3": found[0].v3, "v3_origin": f"pids_{found[0].id}"
            }
            if found[0].aop:
                d['aop'] = found[0].aop
            return d
        raise MoreThanOneRecordFoundError(
            "Found more than one: {}".format(
                " ".join([doc.id for doc in found])
            )
        )

    def _get_document_from_pid_versions_table(self):
        """
        Busca registro na tabela `pid_versions`:
            id
            v2 (ou aop)
        """
        params = {
            "v2": self.input_data.get("v2") or '',
        }

        try:
            found = self._db_query(PidVersion, **params)
        except Exception as e:
            raise QueryDocumentInPidVersionsTableError(
                "Error querying doc in `pid_versions`: %s %s" % (params, e)
            )

        if len(found) == 0:
            # não está registrado
            return None

        if len(found) == 1:
            return {"v3": found[0].v3,
                    "v3_origin": f"pid_versions_{found[0].id}"}

        raise MoreThanOneRecordFoundError(
            "Found more than one: {}".format(
                " ".join([doc.id for doc in found])
            )
        )

    def _get_unique_v3(self):
        while True:
            generated = self._generate_v3()
            if not self.is_registered_v3(generated):
                return generated

    def is_registered_v3(self, v3):
        exceptions = []
        for table_class in (Documents, NewPidVersion, PidVersion):
            try:
                registered = self._db_query(table_class, **{"v3": v3})
            except Exception as e:
                exceptions.append(str(e))
                continue

            if registered:
                return True
        if len(exceptions) == 3:
            raise IsRegisteredV3Error(" | ".join(exceptions))
        return False

    def _prepare_record_to_save(self, data, recovered_data=None):
        recovered_data = recovered_data or {}

        if not recovered_data.get("v3"):
            if not data["v3"] or self.is_registered_v3(data["v3"]):
                data["v3"] = self._get_unique_v3()
                data["v3_origin"] = "generated"

        data.update(recovered_data or {})
        return data

    def _save_record(self, data):
        try:
            doc = Documents(**data)
            self._session.add(doc)
            self._session.commit()
        except IntegrityError as e:
            self._session.rollback()
            raise AlreadyRegisteredDocumentError("Rollback: %s" % str(e))
        except SQLAlchemyError as e:
            self._session.rollback()
            raise RegistrationError("Rollback: %s" % str(e))
        return doc.as_dict

    def _register_doc(self, recovered_data=None):
        data = self.input_data.copy()
        record_data = self._prepare_record_to_save(data, recovered_data)
        return self._save_record(record_data)

    def manage_docs(self):
        """
        Obtém registro consultando com dados além de v2, aop, doi, filename
        Cria / atualiza o registro
        Retorna dicionário cujas chaves são:
            input, found, saved, error, warning
        """
        recovered_data = {}
        response = {}
        response["input"] = self.input_data
        try:
            # busca o documento com dados do fascículo + pid v2
            registered = self._get_document_published_in_an_issue(
                self.input_data.get("v2")
            )
            if not registered:
                # busca o documento com dados do fascículo, sem pid v2
                registered = self._get_document_published_in_an_issue()
            if not registered:
                # recupera dados de aop, se aplicável, ou das outras tabelas
                recovered_data = (
                    self._get_document_aop_version() or
                    self._get_document_from_pids_table() or
                    self._get_document_from_pid_versions_table()
                )
        except MoreThanOneRecordFoundError as e:
            # melhor criar novo registro e tratar da ambiguidade posteriormente
            # que recuperar erroneamente o registro
            response['exception'] = {
                'action': 'manage_docs: querying record',
                'type': str(type(e)),
                'msg': str(e),
            }
            registered = None
        if registered:
            if self.input_data["v2"] == registered["v2"]:
                response['registered'] = registered
            else:
                recovered_data = {
                    "v3": registered["v3"],
                    "v3_origin": registered["v3_origin"],
                }
                if registered.get("aop"):
                    recovered_data["aop"] = registered["aop"]
                response['created'] = self._register_doc(recovered_data)
            return response
        try:
            response['created'] = self._register_doc(recovered_data)
        except AlreadyRegisteredDocumentError as e:
            pass
        except RegistrationError as e:
            response['exception'] = {
                'action': 'manage_docs: registering document',
                'type': str(type(e)),
                'msg': str(e)
            }
        except Exception as e:
            response['exception'] = {
                'action': 'manage_docs: registering document',
                'type': str(type(e)),
                'msg': str(e)
            }
        return response


class Manager:
    def __init__(self, name, timeout=None, _engine_args={}):
        self._name = name
        self._engine_args = {"pool_timeout": timeout} if timeout else {}
        self._engine_args.update({"pool_size": 10, "max_overflow": 20})
        self._engine_args.update(_engine_args)
        self.setup()

    def setup(self):
        self._engine = create_engine(
            self._name, logging_name='pid_manager', **self._engine_args)
        Base.metadata.create_all(self._engine)
        self.Session = sessionmaker(bind=self._engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise RegistrationError("Rollback: %s" % str(e))
        finally:
            session.close()

    @staticmethod
    def _format_record(registered):
        if registered:
            result = {
                "v3": registered.v3,
                "v2": registered.v2,
            }
            if hasattr(registered, 'created'):
                result.update(
                    {
                        "aop": registered.aop,
                        "doi": registered.doi,
                        "status": registered.status,
                        "filename": registered.filename,
                        "created": registered.created,
                        "updated": registered.updated,
                    }
                )
            return result

    def manage_docs(self, generate_v3, v2, v3, aop, filename, doi,
                    status,
                    pub_year, issue_order,
                    volume, number, suppl,
                    elocation, fpage, lpage,
                    first_author_surname, last_author_surname,
                    article_title, other_pids):
        with self.session_scope() as session:
            doc_manager = DocManager(
                session, generate_v3,
                v2, v3, aop, filename, doi,
                status,
                pub_year, issue_order,
                volume, number, suppl,
                elocation, fpage, lpage,
                first_author_surname, last_author_surname,
                article_title, other_pids)
            result = doc_manager.manage_docs()
        return result

    def manage(self, v2, v3, aop, filename, doi, status, generate_v3):
        """
        Obtém registro consultando por v2, aop, doi, filename
        Cria / atualiza o registro
        Retorna dicionário cujas chaves são:
            input, found, saved, error, warning
        """
        result = {
            "input": {
                "v3": v3,
                "v2": v2,
                "aop": aop,
                "doi": doi,
                "filename": filename,
                "status": status,
            }
        }
        saved = None
        try:
            if not v2:
                raise ValueError("Manager.manage requires parameters: v2")
            with self.session_scope() as session:
                # obtém o registro
                registered = None
                if not registered:
                    registered = self._get_record(
                        session, v2, filename, doi, aop)
                if not registered:
                    registered = self._get_record_old(session, v2, aop)
                if registered:
                    result['registered'] = self._format_record(registered)

                # guarda o registro
                if len(filename) > 80:
                    result['warning'] = {"filename": filename}
                if registered:
                    if not hasattr(registered, 'created'):
                        # versão anterior do schema (v2, v3),
                        # então registrar no novo schema
                        saved = self._register(
                            session, v2, registered.v3, aop,
                            filename, doi, status)
                    else:
                        # já está registraddo no schema novo,
                        # então fazer atualização
                        saved = self._register(
                            session, v2, v3, aop,
                            filename, doi, status, registered)
                else:
                    saved = self._register(
                        session, v2,
                        self.get_unique_v3(session, v3, generate_v3),
                        aop, filename, doi, status)
                if saved:
                    result['saved'] = saved
        except RegistrationError as e:
            result['error'] = str(e)
        except Exception as e:
            result['error'] = str(e)
        finally:
            return result

    def get_unique_v3(self, session, v3, generate_v3):
        unique_v3 = v3 or generate_v3()
        while True:
            exist = bool(
                session.query(NewPidVersion).filter_by(v3=unique_v3).first() or
                session.query(PidVersion).filter_by(v3=unique_v3).first()
            )
            if not exist:
                return unique_v3
            unique_v3 = generate_v3()

    def _register(self, session, v2, v3, aop, filename, doi, status, row=None):
        filename = filename[:80]
        prefix_v2 = v2 and v2[:-5] or ""
        prefix_aop = aop and aop[:-5] or ""
        if row:
            # update
            data = {
                "v2": v2,
                "v3": v3 or row.v3,
                "aop": aop or row.aop,
                "doi": doi or row.doi,
                "filename": filename or row.filename,
                "status": status or row.status,
                "prefix_aop": prefix_aop or row.prefix_aop,
                "prefix_v2": prefix_v2 or row.prefix_v2,
            }
            session.query(NewPidVersion).filter(
                NewPidVersion.id == row.id).update(data)
            return data
        else:
            # create
            data = NewPidVersion(
                v2=v2,
                v3=v3,
                aop=aop or "",
                filename=filename or "",
                doi=doi or "",
                status=status or "",
                prefix_aop=prefix_aop,
                prefix_v2=prefix_v2,
            )
            session.add(data)
            return self._format_record(data)

    def _get_record_by_v3(self, session, v3, v2, filename, doi, aop):
        if not v3:
            return

        for rec in session.query(NewPidVersion).filter_by(v3=v3).all():
            if filename and filename == rec.filename:
                return rec
            if doi and doi == rec.doi:
                return rec
            if aop and aop in (rec.v2, rec.aop):
                return rec
            if v2 and v2 in (rec.v2, rec.aop):
                return rec

        for rec in session.query(PidVersion).filter_by(v3=v3).all():
            if aop and aop == rec.v2:
                return rec
            if v2 and v2 == rec.v2:
                return rec

    def _get_record(self, session, v2, filename, doi, aop):
        record = None
        if filename:
            if not record and doi:
                record = session.query(NewPidVersion).filter_by(
                    doi=doi, filename=filename
                ).first()
            if not record and aop:
                prefix = aop[:-5]
                record = (
                    session.query(NewPidVersion).filter_by(
                        prefix_v2=prefix, filename=filename
                    ).first() or
                    session.query(NewPidVersion).filter_by(
                        prefix_aop=prefix, filename=filename
                    ).first()
                )
            if not record and v2:
                prefix = v2[:-5]
                record = (
                    session.query(NewPidVersion).filter_by(
                        prefix_v2=prefix, filename=filename
                    ).first() or
                    session.query(NewPidVersion).filter_by(
                        prefix_aop=prefix, filename=filename
                    ).first()
                )
        if not record and doi:
            record = session.query(NewPidVersion).filter_by(doi=doi).first()
        if not record and aop:
            record = (
                session.query(NewPidVersion).filter_by(v2=aop).first() or
                session.query(NewPidVersion).filter_by(aop=aop).first()
            )
        if not record and v2:
            record = (
                session.query(NewPidVersion).filter_by(v2=v2).first() or
                session.query(NewPidVersion).filter_by(aop=v2).first()
            )
        return record

    def _get_record_old(self, session, v2, aop):
        i = 0
        record = None
        if aop:
            for rec in session.query(PidVersion).filter_by(v2=aop).all():
                if rec.id > i:
                    i = rec.id
                    record = rec
        if v2:
            for rec in session.query(PidVersion).filter_by(v2=v2).all():
                if rec.id > i:
                    i = rec.id
                    record = rec
        return record

