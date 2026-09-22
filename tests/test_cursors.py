import unittest
import uuid
from datetime import date, datetime
from decimal import Decimal

from sync_etl.cursors import CursorError, decode_cursor, encode_cursor, json_safe


class CursorCodecTests(unittest.TestCase):
    def test_supported_values_round_trip_without_losing_type(self):
        values = [
            42,
            Decimal("12.30"),
            1.5,
            datetime(2026, 1, 2, 3, 4, 5),
            date(2026, 1, 2),
            uuid.uuid4(),
            "cursor-1",
        ]
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(decode_cursor(encode_cursor(value)), value)

    def test_null_and_boolean_cursors_are_rejected(self):
        with self.assertRaises(CursorError):
            encode_cursor(None)
        with self.assertRaises(CursorError):
            encode_cursor(True)

    def test_business_rows_become_json_safe(self):
        row = {
            "price": Decimal("10.50"),
            "created": datetime(2026, 1, 2, 3, 4, 5),
            "id": uuid.UUID("00000000-0000-0000-0000-000000000001"),
        }
        self.assertEqual(
            json_safe(row),
            {
                "price": "10.50",
                "created": "2026-01-02T03:04:05",
                "id": "00000000-0000-0000-0000-000000000001",
            },
        )


if __name__ == "__main__":
    unittest.main()
