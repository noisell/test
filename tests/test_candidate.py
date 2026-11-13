"""Добавляйте сюда собственные проверки или создавайте другие test_*.py файлы."""

import unittest

from catalog_mirror import ExponentialBackoff


class CandidateSmokeTest(unittest.TestCase):
    def test_backoff_is_capped(self):
        self.assertEqual(ExponentialBackoff(5, 20).delay_seconds(10), 20)

