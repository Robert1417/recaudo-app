import unittest

from recaudo_rules import apply_low_ratio_pp_cap


class ApplyLowRatioPpCapTests(unittest.TestCase):
    def test_caps_prediction_above_limit_when_ratio_is_below_point_one(self):
        self.assertEqual(apply_low_ratio_pp_cap(0.82, 0.08), (0.75, True))

    def test_caps_prediction_above_limit_when_ratio_equals_point_one(self):
        self.assertEqual(apply_low_ratio_pp_cap(0.90, 0.10), (0.75, True))

    def test_keeps_prediction_at_or_below_cap_for_low_ratio(self):
        self.assertEqual(apply_low_ratio_pp_cap(0.70, 0.05), (0.70, False))
        self.assertEqual(apply_low_ratio_pp_cap(0.75, 0.10), (0.75, False))

    def test_keeps_prediction_when_ratio_is_above_point_one(self):
        self.assertEqual(apply_low_ratio_pp_cap(0.90, 0.1001), (0.90, False))


if __name__ == "__main__":
    unittest.main()
