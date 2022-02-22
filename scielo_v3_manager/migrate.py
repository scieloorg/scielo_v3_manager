import os
import argparse
import json

from scielo_v3_manager.v3_gen import generates
from scielo_v3_manager.pid_manager import Manager


def read_jsonl(input_jsonl_file_path):
    with open(input_jsonl_file_path) as fp:
        for row in fp.readlines():
            try:
                data = json.loads(row)
            except json.JSONDecodeError as e:
                response = {
                    'exception_type': str(type(e)),
                    'exception_msg': str(str(e)),
                    'row': row,
                }
                yield response
            else:
                yield data


def _insert_into_docs_table(manager, data):
    """
    {"v2": "S1983-46322020000100501", "v3": "HwmhLST8hZ8Zdm5PtHWhthH",
     "aop": "", "filename": "", "doi": "10.1590/PBOCI.2020.131",
     "pub_year": "2020", "issue_order": null, "elocation": "", "fpage": "",
     "lpage": "", "first_author_surname": "VIEIRA",
     "article_title": "DIFFERENCES IN PROTEOMIC PROFILES BETWEEN CARIES FREE AND CARIES AFFECTED CHILDREN",
     "other_pids": "S1983-46322020000100501 HwmhLST8hZ8Zdm5PtHWhthH"}
    """
    try:
        registered = manager.manage_docs(generates, **data)
    except Exception as e:
        response = {
            'exception_type': str(type(e)),
            'exception_msg': str(str(e)),
            'data': data,
        }
    else:
        response = registered
    return response


def insert_into_table(manager,
                      input_jsonl_file_path, log_file_path):
    with open(log_file_path, "w") as fpl:
        fpl.write("")

    for data in read_jsonl(input_jsonl_file_path):
        if data.get("exception_type"):
            response = data
        else:
            response = _insert_into_docs_table(manager, data)

        try:
            del response['registered']['updated']
            del response['registered']['created']
        except KeyError:
            pass
        try:
            del response['saved']['updated']
            del response['saved']['created']
        except KeyError:
            pass

        print(response)
        with open(log_file_path, "a") as fpl:
            fpl.write(f"{json.dumps(response)}\n")


def main():
    parser = argparse.ArgumentParser(description="PID v3 importer")
    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command",
    )

    register_parser = subparsers.add_parser(
        "register",
        help=(
            "Populate table"
        )
    )
    register_parser.add_argument(
        "input_jsonl_file_path",
        help=(
            "/path/data.jsonl"
        )
    )
    register_parser.add_argument(
        "db_sql",
        help=(
            "postgresql+psycopg2://user@localhost:5432/pid_manager"
        )
    )
    register_parser.add_argument(
        "log_file_path",
        help=(
            "/path/result.jsonl"
        )
    )

    args = parser.parse_args()
    if args.command == "register":
        manager = Manager(args.db_sql, 20000)
        insert_into_table(
            manager, args.input_jsonl_file_path, args.log_file_path,
        )
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
