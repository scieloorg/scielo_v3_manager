from unittest import TestCase
from unittest.mock import patch, ANY, call

from scielo_v3_manager.pid_manager import DocManager


class DocManagerTest(TestCase):

    @patch('scielo_v3_manager.pid_manager.DocManager._load_input')
    def test_load_input_is_called_with_parameters(self, mock_l):
        doc = DocManager(ANY, ANY,
                         'S1234-567820201111101234', 'v3', 'aop',
                         'filename', 'doi',
                         'status',
                         'pub_year', 'issue_order',
                         'volume', 'number', 'suppl',
                         'elocation', 'fpage', 'lpage',
                         'first_author_surname', 'last_author_surname',
                         'article_title', 'other_pids',
                         )
        mock_l.assert_called_once_with(
            'S1234-567820201111101234', 'v3', 'aop',
            'filename', 'doi',
            ANY,
            'pub_year', 'issue_order',
            'volume', 'number', 'suppl',
            'elocation', 'fpage', 'lpage',
            'first_author_surname', 'last_author_surname',
            'article_title', 'other_pids',
        )

    def test_load_input_updates_input_data(self):
        doc = DocManager(ANY, ANY,
                         'S1234-56782020111101234', 'v3', 'aop',
                         'filename', 'doi',
                         'status',
                         'pub_year', '1111',
                         'volume', 'number', 'suppl',
                         'elocation', 'fpage', 'lpage',
                         'first_author_surname', 'last_author_surname',
                         'article_title', 'other_pids',
                         )
        expected = {
            'v2': 'S1234-56782020111101234',
            'v3': 'v3',
            'v3_origin': 'provided',
            'aop': 'aop',
            'filename': 'filename',
            'doi': 'DOI',
            'status': 'STATUS',
            'pub_year': 'pub_year',
            'issue_order': '1111',
            'volume': 'VOLUME',
            'number': 'NUMBER',
            'suppl': 'SUPPL',
            'elocation': 'ELOCATION',
            'fpage': 'FPAGE',
            'lpage': 'LPAGE',
            'first_author_surname': 'FIRST_AUTHOR_SURNAME',
            'last_author_surname': 'LAST_AUTHOR_SURNAME',
            'article_title': 'ARTICLE_TITLE',
            'issn': '1234-5678',
            'other_pids': 'other_pids',
        }
        result = doc.input_data
        self.assertDictEqual(expected, result)


def get_mock_document(id, data=None):
    """
    Cria um documento para testes
    """
    _data = {
        "v2": "S1807-59322020000100415",
        "v3": "",
        "aop": "", "filename": "1807-5932-clin-75-e2022.xml",
        "doi": "10.6061/CLINICS/2020/E2022",
        "pub_year": "2020",
        "issue_order": "0001",
        "volume": "75",
        "number": "",
        "suppl": "",
        "elocation": "",
        "fpage": "415",
        "lpage": "",
        "first_author_surname": "ALMEIDA",
        "last_author_surname": "MENDONCA",
        "article_title": "ADRENAL INSUFFICIENCY AND GLUCOCORTICOID USE DURIN",
        "other_pids": "cln_75p1 gtQgKWgKNW8rrtTjF7mv3Ld S1807-59322020000100415",
        "status": "",
    }
    if id:
        _data["id"] = id
        _data["v3"] = "registered_v3"
        _data["v3_origin"] = "registered"
    _data.update(data or {})
    return _data


class PrepareRecordToSaveTest(TestCase):

    def test_prepare_data_to_save__uses_v3_from_recovered_data(self):

        def _get_unique_v3():
            return 'unique_pid_v3'

        doc_data = get_mock_document(id=None)
        doc_manager = DocManager(ANY, ANY, **doc_data)
        doc_manager._get_unique_v3 = _get_unique_v3

        expected = {
            "original_data": "any",
            "v3": "v3_from_recovered_data",
            "v3_origin": "v3_origin_from_recovered_data",
        }

        data = {"original_data": "any", "v3": ""}
        recovered_data = {
            "v3": "v3_from_recovered_data",
            "v3_origin": "v3_origin_from_recovered_data",
        }
        result = doc_manager._prepare_record_to_save(data, recovered_data)
        self.assertDictEqual(expected, result)

    def test_prepare_data_to_save__uses_recovered_data_instead_of_doc_data(self):

        def _get_unique_v3():
            return 'unique_pid_v3'

        doc_data = get_mock_document(id=None)
        doc_manager = DocManager(ANY, ANY, **doc_data)
        doc_manager._get_unique_v3 = _get_unique_v3

        expected = {
            "original_data": "any",
            "v3": "v3_from_recovered_data",
            "v3_origin": "v3_origin_from_recovered_data",
            "aop": "aop_recovered",
        }

        data = {
            "original_data": "any",
            "v3": "este_pid_sera_ignorado",
            "aop": "",
        }
        recovered_data = {
            "v3": "v3_from_recovered_data",
            "v3_origin": "v3_origin_from_recovered_data",
            "aop": "aop_recovered",
        }
        result = doc_manager._prepare_record_to_save(data, recovered_data)
        self.assertDictEqual(expected, result)


@patch('scielo_v3_manager.pid_manager.DocManager._get_document_published_in_an_issue')
@patch('scielo_v3_manager.pid_manager.DocManager._get_document_aop_version')
@patch('scielo_v3_manager.pid_manager.DocManager._get_document_from_pids_table')
@patch('scielo_v3_manager.pid_manager.DocManager._get_document_from_pid_versions_table')
@patch('scielo_v3_manager.pid_manager.DocManager._save_record')
@patch('scielo_v3_manager.pid_manager.DocManager.is_registered_v3')
@patch('scielo_v3_manager.pid_manager.DocManager._get_unique_v3')
class RegisterArticlesTest(TestCase):

    def test_register_article_which_v2_and_article_metadata_are_not_registered(
            self,
            mock__get_unique_v3,
            mock_is_registered_v3,
            mock__save_record,
            mock__get_document_from_pid_versions_table,
            mock__get_document_from_pids_table,
            mock__get_document_aop_version,
            mock__get_document_published_in_an_issue,
            ):
        """
        Test registration of v2 and article metadata which are not registered.
        Returns created record data
        """
        mock_is_registered_v3.return_value = False
        mock__get_unique_v3.return_value = "new_pid_v3"

        mock__get_document_from_pid_versions_table.return_value = None
        mock__get_document_from_pids_table.return_value = None
        mock__get_document_aop_version.return_value = None
        mock__get_document_published_in_an_issue.side_effect = [
            None, None
        ]
        doc_data = get_mock_document(id=None)

        doc_manager = DocManager(ANY, ANY, **doc_data)
        result = doc_manager.manage_docs()

        doc_data['issn'] = doc_data['v2'][1:10]
        doc_data['v3'] = "new_pid_v3"
        doc_data['v3_origin'] = "generated"
        mock__save_record.assert_called_once_with(doc_data)
        self.assertIsNotNone(result["created"])

    def test_register_article_which_v2_and_article_metadata_are_registered(
            self,
            mock__get_unique_v3,
            mock_is_registered_v3,
            mock__save_record,
            mock__get_document_from_pid_versions_table,
            mock__get_document_from_pids_table,
            mock__get_document_aop_version,
            mock__get_document_published_in_an_issue,
            ):
        """
        Test registration of v2 and article metadata which are registered
        and they are in the same record.
        Returns registered record data
        """
        mock_is_registered_v3.return_value = False
        mock__get_unique_v3.return_value = "new_pid_v3"

        REGISTERED_RECORD_DATA = get_mock_document(id=14)
        mock__get_document_published_in_an_issue.return_value = REGISTERED_RECORD_DATA

        doc_data = get_mock_document(id=None)
        doc_manager = DocManager(ANY, ANY, **doc_data)
        result = doc_manager.manage_docs()

        mock__get_document_from_pid_versions_table.assert_not_called()
        mock__get_document_from_pids_table.assert_not_called()
        mock__get_document_aop_version.assert_not_called()
        mock__save_record.assert_not_called()
        self.assertDictEqual(REGISTERED_RECORD_DATA, result["registered"])

    def test_register_article_which_v2_is_not_registered_and_article_metadata_is_registered(
            self,
            mock__get_unique_v3,
            mock_is_registered_v3,
            mock__save_record,
            mock__get_document_from_pid_versions_table,
            mock__get_document_from_pids_table,
            mock__get_document_aop_version,
            mock__get_document_published_in_an_issue,
            ):
        """
        Test registration of document which v2 is not registered and
            article metadata is registered.
        There will be 2 records with the same article metadata, but different
            values for v2
        Returns created record
        """
        mock_is_registered_v3.return_value = False
        mock__get_unique_v3.return_value = "new_pid_v3"

        REGISTERED_RECORD_DATA = get_mock_document(id=15)
        # busca o documento com dados do fascículo + pid v2 -> nao encontra
        # busca o documento com dados do fascículo sem pid v2 -> encontra
        mock__get_document_published_in_an_issue.side_effect = [
            None,
            REGISTERED_RECORD_DATA,
        ]

        doc_data = get_mock_document(id=None)
        doc_data["v2"] = "S1807-59322020000100490"

        doc_manager = DocManager(ANY, ANY, **doc_data)
        result = doc_manager.manage_docs()

        mock__get_document_from_pid_versions_table.assert_not_called()
        mock__get_document_from_pids_table.assert_not_called()
        mock__get_document_aop_version.assert_not_called()

        doc_data['issn'] = doc_data['v2'][1:10]
        doc_data['v3'] = "registered_v3"
        doc_data['v3_origin'] = "registered"
        mock__save_record.assert_called_once_with(doc_data)
        self.assertIsNotNone(result["created"])

