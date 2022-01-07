# -*- coding: utf-8 -*-


from tests.test_parse_file import diagnostics_file


def test_dump_json():
    import pyftdc
    p = pyftdc.FTDCParser()

    p.dump_file_as_json(diagnostics_file, '/tmp/test_all.log')


def test_dump_json_ts():
    import pyftdc
    p = pyftdc.FTDCParser()

    p.dump_file_as_json(diagnostics_file, '/tmp/test_ts.log')
