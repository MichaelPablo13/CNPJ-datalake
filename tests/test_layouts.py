import unittest

from src.cnpj_datalake.domain.layouts import get_layout_columns, normalize_file_type


class LayoutTests(unittest.TestCase):
    def test_normalize_aliases(self):
        self.assertEqual(normalize_file_type("empresa"), "empresas")
        self.assertEqual(normalize_file_type("SOCIOS"), "socios")
        self.assertEqual(normalize_file_type("Municipio"), "municipios")

    def test_layout_columns_estabelecimentos_has_32(self):
        cols = get_layout_columns("estabelecimentos")
        self.assertEqual(len(cols), 32)
        self.assertEqual(cols[-2:], ["extra_col_1", "extra_col_2"])


if __name__ == "__main__":
    unittest.main()
