import logging
from datetime import datetime
from contextlib import contextmanager

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime,
    UniqueConstraint, create_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


Base = declarative_base()

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)


class MoreThanOneRecordFoundError(Exception):
    ...


class RegistrationError(Exception):
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
    aop = Column(String(23), index=True)

    filename = Column(String(80), index=True)
    doi = Column(String(80), index=True)
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

    article_title = Column(String(100))
    other_pids = Column(String(200))
    status = Column(String(15))

    created = Column(DateTime, default=datetime.utcnow())
    updated = Column(DateTime, default=datetime.utcnow(), onupdate=datetime.now())

    __table_args__ = (
        UniqueConstraint('v3', name='_docs_'),
    )

    def __repr__(self):
        return (
            '<Documents(v2="%s", v3="%s", aop="%s", doi="%s", filename="%s")>' %
            (self.v2, self.v3, self.aop, self.doi, self.filename)
        )

    @property
    def as_dict(self):
        NAMES = (
            'v2', 'v3', 'aop', 'filename', 'doi',
            'pub_year', 'issue_order', 'elocation', 'fpage', 'lpage',
            'first_author_surname', 'last_author_surname',
            'article_title', 'other_pids',
            'issn',
            'id', 'updated', 'created',
        )
        values = (
            self.v2, self.v3, self.aop, self.filename, self.doi,
            self.pub_year, self.issue_order, self.elocation, self.fpage, self.lpage,
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
        issue_order = str(issue_order)
        issue_order = (issue_order[4:] or issue_order).zfill(4)
        self._load_input(
            v2[:23], v3, aop[:23], filename[:80], doi,
            status[:15],
            pub_year, issue_order,
            volume, number, suppl,
            elocation, fpage, lpage,
            first_author_surname[:30],
            last_author_surname[:30],
            article_title[:100], other_pids[:200]
        )

    def _load_input(self, v2, v3, aop, filename, doi,
                    status,
                    pub_year, issue_order,
                    volume, number, suppl,
                    elocation, fpage, lpage,
                    first_author_surname, last_author_surname,
                    article_title, other_pids,
                    ):
        _values = (
            v2, aop, doi,
            status,
            pub_year, issue_order,
            volume, number, suppl,
            elocation, fpage, lpage,
            first_author_surname, last_author_surname,
            article_title,
        )
        values = []
        for vv in _values:
            try:
                values.append(vv.upper())
            except (AttributeError, TypeError, ValueError):
                values.append(vv)
        values.append(v2[1:10])
        values.append(v3)
        values.append(other_pids)
        values.append(filename)
        self._input_data = dict(zip(self._input_attributes, values))

    @property
    def input_data(self):
        return self._input_data

    @property
    def _input_attributes(self):
        return (
            'v2', 'aop', 'doi',
            'status',
            'pub_year', 'issue_order', 'elocation', 'fpage', 'lpage',
            'first_author_surname', 'last_author_surname',
            'article_title',
            'issn', 'v3', 'other_pids', 'filename',
        )

    @property
    def _doc_attributes(self):
        return (
            'pub_year', 'issue_order', 'elocation', 'fpage', 'lpage', 'doi',
            'first_author_surname', 'last_author_surname', 'issn',
        )

    def _db_query(self, **kwargs):
        return self._session.query(Documents).filter_by(**kwargs).all()

    def _get_document(self):
        # {'aop', 'article_title', 'filename', 'other_pids', 'status', 'v2', 'v3'}
        d = {
            k: self.input_data.get(k) or ''
            for k in self._doc_attributes
        }
        found = self._db_query(**d)

        if len(found) == 0:
            return None

        v3 = self.input_data.get("v3") or ''
        v2 = self.input_data.get("v2") or ''
        aop = self.input_data.get("aop") or ''
        filename = self.input_data.get("filename") or ''

        matches = []
        for item in found:
            if v3 and v3 == item.v3:
                matches.append(item)
                continue

            if filename == item.filename:
                matches.append(item)
                continue

            item_pids = f"{item.other_pids} {item.v2} {item.aop}"

            if aop and aop in item_pids:
                matches.append(item)
                continue

            if v2 and v2 in item_pids:
                matches.append(item)
                continue

        if len(matches) == 0:
            return None

        if len(matches) == 1:
            return matches[0]

        raise MoreThanOneRecordFoundError(
            "Found more than one: {}".format(
                " ".join([doc.id for doc in matches])
            )
        )

    def _get_unique_v3(self, v3):
        while True:
            unique_v3 = v3 or self._generate_v3()
            registered = self._db_query(**{"v3": unique_v3})
            if not registered:
                return unique_v3

    def _register_doc(self):
        # create
        data = Documents(
            issn=self.input_data.get("issn") or '',
            v2=self.input_data.get("v2") or '',
            v3=self._get_unique_v3(self.input_data.get("v3")),
            aop=self.input_data.get("aop") or '',
            filename=self.input_data.get("filename") or '',
            doi=self.input_data.get("doi") or '',
            pub_year=self.input_data.get("pub_year") or '',
            issue_order=self.input_data.get("issue_order") or '',
            volume=input_data.get("volume") or '',
            number=input_data.get("number") or '',
            suppl=input_data.get("suppl") or '',
            elocation=self.input_data.get("elocation") or '',
            fpage=self.input_data.get("fpage") or '',
            lpage=self.input_data.get("lpage") or '',
            first_author_surname=self.input_data.get("first_author_surname") or '',
            last_author_surname=self.input_data.get("last_author_surname") or '',
            article_title=(self.input_data.get("article_title") or ''),
            other_pids=(self.input_data.get("other_pids") or ''),
            status=(self.input_data.get("status") or ''),
        )
        self._session.add(data)

        try:
            self._session.commit()
        except SQLAlchemyError as e:
            self._session.rollback()
            raise RegistrationError("Rollback: %s" % str(e))
        return data.as_dict

    def manage_docs(self):
        """
        Obtém registro consultando com dados além de v2, aop, doi, filename
        Cria / atualiza o registro
        Retorna dicionário cujas chaves são:
            input, found, saved, error, warning
        """
        response = {}
        response["input"] = self.input_data
        try:
            registered = self._get_document()
        except MoreThanOneRecordFoundError as e:
            # melhor criar novo registro e tratar da ambiguidade posteriormente
            # que recuperar erroneamente o registro
            response['exception'] = {
                'type': str(type(e)),
                'msg': str(e)
            }
            registered = None
        if registered:
            response['registered'] = registered.as_dict
            return response
        try:
            response['saved'] = self._register_doc()
        except RegistrationError as e:
            response['exception'] = {
                'type': str(type(e)),
                'msg': str(e)
            }
        except Exception as e:
            response['exception'] = {
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
        if len(issue_order) > 4:
            issue_order = issue_order[4:].zfill(4)
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

