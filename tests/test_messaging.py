import tempfile
import unittest
from pathlib import Path

from messaging import ProcessedEvents, decode_order
from producer import make_order


class OrderMessageTests(unittest.TestCase):
    def test_order_has_required_fields(self):
        order = make_order(1, "event-1")
        self.assertEqual(order["event_id"], "event-1")
        self.assertEqual(set(order), {"event_id", "order_id", "item_id", "quantity"})

    def test_decode_rejects_invalid_quantity(self):
        with self.assertRaisesRegex(ValueError, "positive integer"):
            decode_order(b'{"event_id":"e","order_id":"o","item_id":"i","quantity":0}')

    def test_processed_events_survive_new_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.txt"
            self.assertTrue(ProcessedEvents(path).claim("event-1"))
            self.assertFalse(ProcessedEvents(path).claim("event-1"))


if __name__ == "__main__":
    unittest.main()
