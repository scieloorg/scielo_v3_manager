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
        mock_l.asset_called_once_with(
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


def get_mock_document(data=None):
    _data = {
        "v2": "S1807-59322020000100415",
        "v3": "gtQgKWgKNW8rrtTjF7mv3Ld",
        "aop": "", "filename": "1807-5932-clin-75-e2022.xml",
        "doi": "10.6061/CLINICS/2020/E2022",
        "pub_year": "2020",
        "issue_order": "0001",
        "volume": "75",
        "number": "",
        "suppl": "",
        "elocation": "",
        "fpage": "",
        "lpage": "",
        "first_author_surname": "ALMEIDA",
        "last_author_surname": "MENDONCA",
        "article_title": "ADRENAL INSUFFICIENCY AND GLUCOCORTICOID USE DURIN",
        "other_pids": "cln_75p1 gtQgKWgKNW8rrtTjF7mv3Ld S1807-59322020000100415",
        "status": "",
    }
    _data.update(data or {})
    return _data


@patch('scielo_v3_manager.pid_manager.DocManager._get_document_published_in_an_issue_attributes')
@patch('scielo_v3_manager.pid_manager.DocManager._get_document_aop_version')
@patch('scielo_v3_manager.pid_manager.DocManager._get_document_from_pids_table')
@patch('scielo_v3_manager.pid_manager.DocManager._get_document_from_pid_versions_table')
@patch('scielo_v3_manager.pid_manager.DocManager._register_doc')
class RegisterArticlesTest(TestCase):

    def test_register_article_which_v2_and_article_metadata_are_not_registered(
            self,
            mock__register_doc,
            mock__get_document_from_pid_versions_table,
            mock__get_document_from_pids_table,
            mock__get_document_aop_version,
            mock__get_document_published_in_an_issue_attributes,
            ):
        """
        Test registration of v2 and article metadata which are not registered.
        Returns created data
        """
        mock__get_document_from_pid_versions_table.return_value = None
        mock__get_document_from_pids_table.return_value = None
        mock__get_document_aop_version.return_value = None
        mock__get_document_published_in_an_issue_attributes.side_effect = [
            None, None
        ]
        REGISTER_DOC_RESULT = get_mock_document({"id": 14})
        mock__register_doc.return_value = REGISTER_DOC_RESULT
        doc_data = get_mock_document()

        doc_manager = DocManager(ANY, ANY, **doc_data)
        result = doc_manager.manage_docs()
        mock__register_doc.asset_called_once_with({})
        self.assertDictEqual(REGISTER_DOC_RESULT, result["created"])

    def test_register_article_which_v2_and_article_metadata_are_registered(
            self,
            mock__register_doc,
            mock__get_document_from_pid_versions_table,
            mock__get_document_from_pids_table,
            mock__get_document_aop_version,
            mock__get_document_published_in_an_issue_attributes,
            ):
        """
        Test registration of v2 and article metadata which are registered.
        Returns registered data
        """
        RESULT = get_mock_document({"id": 14})
        mock__get_document_published_in_an_issue_attributes.return_value = RESULT
        doc_data = get_mock_document()

        doc_manager = DocManager(ANY, ANY, **doc_data)
        result = doc_manager.manage_docs()

        mock__get_document_from_pid_versions_table.asset_not_called()
        mock__get_document_from_pids_table.asset_not_called()
        mock__get_document_aop_version.asset_not_called()
        mock__register_doc.asset_not_called()
        self.assertDictEqual(RESULT, result["registered"])
