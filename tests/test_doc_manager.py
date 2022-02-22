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
