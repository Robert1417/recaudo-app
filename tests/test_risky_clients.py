import unittest

from risky_clients import extract_risky_references, normalize_reference


class RiskyClientSheetTests(unittest.TestCase):
    def test_extracts_only_references_marked_as_atrasado(self):
        values = [
            ["Referencia", "Nombre", "Tipo de cliente"],
            ["123", "Cliente A", "Atrasado"],
            ["456", "Cliente B", "Al día"],
            ["789.0", "Cliente C", " atrasado "],
        ]

        self.assertEqual(extract_risky_references(values), {"123", "789"})

    def test_requires_expected_sheet_headers(self):
        with self.assertRaisesRegex(RuntimeError, "Referencia y Tipo de cliente"):
            extract_risky_references([["Referencia", "Estado"], ["123", "Atrasado"]])

    def test_normalizes_reference_loaded_as_decimal(self):
        self.assertEqual(normalize_reference("123.0"), "123")


if __name__ == "__main__":
    unittest.main()
